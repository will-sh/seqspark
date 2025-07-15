#!/bin/bash

# SeqSpark 性能优化脚本
# 专为处理大型压缩FASTQ文件优化

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

usage() {
    cat << EOF
SeqSpark 性能优化脚本

用法: $0 [选项] -i 输入文件 -n 采样数量 -o 输出文件

选项:
    -i, --input         输入FASTQ文件 (必需)
    -o, --output        输出文件 (必需)
    -n, --number        采样数量 (必需)
    -s, --seed          随机种子 (默认: 100)
    -t, --temp-dir      临时目录 (默认: /tmp)
    -k, --keep-temp     保留临时文件
    -m, --memory        Spark内存 (默认: 16g)
    -c, --cores         CPU核心数 (默认: 自动检测)
    -h, --help          显示帮助信息

示例:
    # 优化处理大型压缩文件
    $0 -i K562_100M_R1.fastq.gz -n 50000000 -o sampled.fastq.gz
    
    # 指定更多内存和核心
    $0 -i K562_100M_R1.fastq.gz -n 50000000 -o sampled.fastq.gz -m 32g -c 16

EOF
}

# 默认参数
INPUT_FILE=""
OUTPUT_FILE=""
SAMPLE_NUMBER=0
SEED=100
TEMP_DIR="/tmp"
KEEP_TEMP=false
MEMORY="16g"
CORES=$(nproc)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--input)
            INPUT_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -n|--number)
            SAMPLE_NUMBER="$2"
            shift 2
            ;;
        -s|--seed)
            SEED="$2"
            shift 2
            ;;
        -t|--temp-dir)
            TEMP_DIR="$2"
            shift 2
            ;;
        -k|--keep-temp)
            KEEP_TEMP=true
            shift
            ;;
        -m|--memory)
            MEMORY="$2"
            shift 2
            ;;
        -c|--cores)
            CORES="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "未知选项: $1"
            usage
            exit 1
            ;;
    esac
done

# 验证必需参数
if [[ -z "$INPUT_FILE" || -z "$OUTPUT_FILE" || $SAMPLE_NUMBER -eq 0 ]]; then
    print_error "缺少必需参数"
    usage
    exit 1
fi

# 检查输入文件
if [[ ! -f "$INPUT_FILE" ]]; then
    print_error "输入文件不存在: $INPUT_FILE"
    exit 1
fi

# 创建临时目录
TEMP_WORK_DIR="$TEMP_DIR/seqspark_$$"
mkdir -p "$TEMP_WORK_DIR"

# 清理函数
cleanup() {
    if [[ "$KEEP_TEMP" == "false" ]]; then
        print_info "清理临时文件..."
        rm -rf "$TEMP_WORK_DIR"
    else
        print_info "临时文件保留在: $TEMP_WORK_DIR"
    fi
}

# 注册清理函数
trap cleanup EXIT

print_info "SeqSpark 性能优化处理开始"
print_info "输入文件: $INPUT_FILE"
print_info "输出文件: $OUTPUT_FILE"
print_info "采样数量: $SAMPLE_NUMBER"
print_info "随机种子: $SEED"
print_info "CPU核心数: $CORES"
print_info "内存配置: $MEMORY"
print_info "临时目录: $TEMP_WORK_DIR"

# 检查文件是否压缩
if [[ "$INPUT_FILE" =~ \.(gz|bz2|xz)$ ]]; then
    print_warn "检测到压缩文件，这会导致单分区处理"
    print_step "方案1: 临时解压缩文件以实现并行处理"
    
    # 获取文件大小
    COMPRESSED_SIZE=$(stat -c%s "$INPUT_FILE" 2>/dev/null || stat -f%z "$INPUT_FILE" 2>/dev/null || echo "0")
    COMPRESSED_SIZE_MB=$((COMPRESSED_SIZE / 1024 / 1024))
    
    print_info "压缩文件大小: ${COMPRESSED_SIZE_MB}MB"
    
    # 估算解压后大小（通常为压缩文件的3-5倍）
    ESTIMATED_SIZE_MB=$((COMPRESSED_SIZE_MB * 4))
    print_info "估计解压后大小: ${ESTIMATED_SIZE_MB}MB"
    
    # 检查磁盘空间
    AVAILABLE_SPACE=$(df "$TEMP_DIR" | tail -1 | awk '{print $4}')
    AVAILABLE_SPACE_MB=$((AVAILABLE_SPACE / 1024))
    
    if [[ $ESTIMATED_SIZE_MB -gt $AVAILABLE_SPACE_MB ]]; then
        print_error "磁盘空间不足! 需要: ${ESTIMATED_SIZE_MB}MB, 可用: ${AVAILABLE_SPACE_MB}MB"
        print_info "尝试方案2: 直接处理压缩文件（性能较低）"
        
        # 直接处理压缩文件，但优化参数
        print_step "使用优化参数处理压缩文件"
        
        python3 "$SCRIPT_DIR/seqspark_sample.py" \
            -i "$INPUT_FILE" \
            -n "$SAMPLE_NUMBER" \
            -s "$SEED" \
            -o "$OUTPUT_FILE" \
            --master "local[$CORES]" \
            --memory "$MEMORY"
    else
        print_info "磁盘空间充足，开始解压缩..."
        
        # 解压缩文件
        UNCOMPRESSED_FILE="$TEMP_WORK_DIR/$(basename "$INPUT_FILE" .gz)"
        
        if [[ "$INPUT_FILE" =~ \.gz$ ]]; then
            print_step "解压缩 gzip 文件..."
            gunzip -c "$INPUT_FILE" > "$UNCOMPRESSED_FILE"
        elif [[ "$INPUT_FILE" =~ \.bz2$ ]]; then
            print_step "解压缩 bzip2 文件..."
            bunzip2 -c "$INPUT_FILE" > "$UNCOMPRESSED_FILE"
        elif [[ "$INPUT_FILE" =~ \.xz$ ]]; then
            print_step "解压缩 xz 文件..."
            unxz -c "$INPUT_FILE" > "$UNCOMPRESSED_FILE"
        fi
        
        print_info "解压缩完成"
        
        # 处理解压后的文件
        print_step "处理解压后的文件以实现并行处理"
        
        python3 "$SCRIPT_DIR/seqspark_sample.py" \
            -i "$UNCOMPRESSED_FILE" \
            -n "$SAMPLE_NUMBER" \
            -s "$SEED" \
            -o "$OUTPUT_FILE" \
            --master "local[$CORES]" \
            --memory "$MEMORY"
    fi
    
else
    print_info "文件未压缩，直接处理"
    
    python3 "$SCRIPT_DIR/seqspark_sample.py" \
        -i "$INPUT_FILE" \
        -n "$SAMPLE_NUMBER" \
        -s "$SEED" \
        -o "$OUTPUT_FILE" \
        --master "local[$CORES]" \
        --memory "$MEMORY"
fi

print_info "处理完成！"
print_info "输出文件: $OUTPUT_FILE"

# 显示文件信息
if [[ -f "$OUTPUT_FILE" ]]; then
    OUTPUT_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE" 2>/dev/null || echo "0")
    OUTPUT_SIZE_MB=$((OUTPUT_SIZE / 1024 / 1024))
    print_info "输出文件大小: ${OUTPUT_SIZE_MB}MB"
    
    # 计算采样的序列数量
    if [[ "$OUTPUT_FILE" =~ \.gz$ ]]; then
        SAMPLED_COUNT=$(gunzip -c "$OUTPUT_FILE" | grep -c "^@" || echo "0")
    else
        SAMPLED_COUNT=$(grep -c "^@" "$OUTPUT_FILE" || echo "0")
    fi
    
    print_info "实际采样序列数: $SAMPLED_COUNT"
fi 