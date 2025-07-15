#!/usr/bin/env python3
"""
SeqSpark - 高性能分布式生物序列采样工具（优化版）

这个优化版本专门解决压缩文件单分区的性能问题：
1. 对压缩文件使用RDD重新分区
2. 支持临时保存为可分割格式
3. 自动优化分区数量
4. 提供更好的性能监控

主要改进：
- 使用RDD repartition解决压缩文件单分区问题
- 支持Snappy压缩格式
- 自动分区数量优化
- 更好的内存管理
"""

from __future__ import print_function
import argparse
import sys
import os
import gzip
import bz2
import lzma
import tempfile
import shutil
from typing import Iterator, Optional, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path
import time

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, rand
    from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
    from pyspark.context import SparkContext
except ImportError:
    print("错误：需要安装PySpark。请运行：pip install pyspark", file=sys.stderr)
    sys.exit(1)

@dataclass
class SequenceRecord:
    """序列记录数据结构"""
    id: str
    name: str
    sequence: str
    quality: Optional[str] = None
    line_number: int = 0
    is_fastq: bool = False

def record_to_dict(record):
    """将SequenceRecord转换为字典的独立函数"""
    return {
        'id': record.id,
        'name': record.name,
        'sequence': record.sequence,
        'quality': record.quality,
        'line_number': record.line_number,
        'is_fastq': record.is_fastq
    }

def format_sequence(row):
    """格式化序列输出"""
    if row['is_fastq']:
        # FASTQ格式
        return f"@{row['name']}\n{row['sequence']}\n+\n{row['quality']}"
    else:
        # FASTA格式
        return f">{row['name']}\n{row['sequence']}"

def get_file_opener(file_path: str):
    """根据文件扩展名选择合适的文件打开器"""
    if file_path.endswith('.gz'):
        return gzip.open
    elif file_path.endswith('.bz2'):
        return bz2.open
    elif file_path.endswith('.xz'):
        return lzma.open
    else:
        return open

def detect_format(file_path: str) -> str:
    """检测文件格式"""
    try:
        # 读取前几行来检测格式
        opener = get_file_opener(file_path)
        with opener(file_path, 'rt') as f:
            lines = [f.readline().strip() for _ in range(10)]
        
        fasta_count = sum(1 for line in lines if line.startswith('>'))
        fastq_count = sum(1 for line in lines if line.startswith('@'))
        
        return 'fastq' if fastq_count > fasta_count else 'fasta'
    except Exception as e:
        print(f"警告：格式检测失败: {e}。默认使用FASTA格式。", file=sys.stderr)
        return 'fasta'

def is_compressed_file(file_path: str) -> bool:
    """检查文件是否被压缩"""
    return file_path.endswith(('.gz', '.bz2', '.xz'))

def estimate_partitions(file_path: str, target_partition_size_mb: int = 128) -> int:
    """估算最适合的分区数量"""
    try:
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        # 对于压缩文件，估算解压后大小（通常为3-5倍）
        if is_compressed_file(file_path):
            file_size_mb *= 4
        
        # 计算分区数量
        partitions = max(1, int(file_size_mb / target_partition_size_mb))
        
        # 限制分区数量在合理范围内
        partitions = min(partitions, 10000)  # 最多10000个分区
        partitions = max(partitions, 2)      # 至少2个分区
        
        return partitions
    except Exception as e:
        print(f"警告：无法估算分区数量: {e}。使用默认值。", file=sys.stderr)
        return 200

def preprocess_compressed_file(spark, file_path: str, temp_dir: str, target_partitions: int = None) -> str:
    """预处理压缩文件，重新分区并保存为可分割格式"""
    print(f"[优化] 检测到压缩文件，开始预处理...")
    
    # 估算分区数量
    if target_partitions is None:
        target_partitions = estimate_partitions(file_path)
    
    print(f"[优化] 目标分区数: {target_partitions}")
    
    # 创建临时目录
    temp_output_dir = os.path.join(temp_dir, "repartitioned_data")
    
    start_time = time.time()
    
    # 读取压缩文件为RDD
    print(f"[优化] 读取压缩文件...")
    rdd = spark.sparkContext.textFile(file_path)
    
    # 重新分区
    print(f"[优化] 重新分区到 {target_partitions} 个分区...")
    repartitioned_rdd = rdd.repartition(target_partitions)
    
    # 保存为可分割格式（使用Snappy压缩）
    print(f"[优化] 保存为可分割格式...")
    try:
        repartitioned_rdd.saveAsTextFile(
            temp_output_dir,
            compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec"
        )
    except Exception as e:
        print(f"[优化] Snappy压缩失败，使用无压缩格式: {e}")
        repartitioned_rdd.saveAsTextFile(temp_output_dir)
    
    elapsed_time = time.time() - start_time
    print(f"[优化] 预处理完成，耗时: {elapsed_time:.2f}秒")
    
    return temp_output_dir

def parse_fasta_partition(lines: Iterator[str]) -> Iterator[SequenceRecord]:
    """解析FASTA格式的分区"""
    current_header = None
    current_id = None
    current_seq = []
    line_number = 0
    
    for line in lines:
        line_number += 1
        line = line.strip()
        
        if line.startswith('>'):
            # 保存之前的记录
            if current_header and current_id:
                yield SequenceRecord(
                    id=current_id,
                    name=current_header,
                    sequence=''.join(current_seq),
                    quality=None,
                    line_number=line_number,
                    is_fastq=False
                )
            
            # 开始新记录
            current_header = line[1:]
            current_id = current_header.split()[0]
            current_seq = []
        elif current_header:
            current_seq.append(line)
    
    # 保存最后一个记录
    if current_header and current_id:
        yield SequenceRecord(
            id=current_id,
            name=current_header,
            sequence=''.join(current_seq),
            quality=None,
            line_number=line_number,
            is_fastq=False
        )

def parse_fastq_partition(lines: Iterator[str]) -> Iterator[SequenceRecord]:
    """解析FASTQ格式的分区"""
    line_array = list(lines)
    i = 0
    
    while i < len(line_array):
        line = line_array[i].strip()
        
        if line.startswith('@'):
            if i + 3 < len(line_array):
                header = line[1:]
                id_part = header.split()[0]
                sequence = line_array[i + 1].strip()
                quality = line_array[i + 3].strip()
                
                yield SequenceRecord(
                    id=id_part,
                    name=header,
                    sequence=sequence,
                    quality=quality,
                    line_number=i + 1,
                    is_fastq=True
                )
                
                i += 4
            else:
                i += 1
        else:
            i += 1

def read_sequences_optimized(spark, file_path: str, format_type: str = 'auto', 
                           use_preprocessing: bool = True, temp_dir: str = None):
    """优化版本的序列读取函数"""
    # 检测格式
    if format_type == 'auto':
        format_type = detect_format(file_path)
    
    # 检查是否需要预处理压缩文件
    if use_preprocessing and is_compressed_file(file_path):
        if temp_dir is None:
            temp_dir = tempfile.mkdtemp(prefix="seqspark_")
        
        # 预处理压缩文件
        processed_file_path = preprocess_compressed_file(spark, file_path, temp_dir)
        
        # 读取预处理后的文件
        text_rdd = spark.sparkContext.textFile(processed_file_path)
        
        # 清理临时文件的函数
        def cleanup_temp():
            try:
                shutil.rmtree(temp_dir)
                print(f"[优化] 清理临时文件: {temp_dir}")
            except Exception as e:
                print(f"[优化] 清理临时文件失败: {e}")
        
        # 注册清理函数
        import atexit
        atexit.register(cleanup_temp)
        
    else:
        # 直接读取文件
        text_rdd = spark.sparkContext.textFile(file_path)
    
    # 解析序列
    if format_type == 'fastq':
        sequences_rdd = text_rdd.mapPartitions(parse_fastq_partition)
    else:
        sequences_rdd = text_rdd.mapPartitions(parse_fasta_partition)
    
    # 转换为DataFrame
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("quality", StringType(), True),
        StructField("line_number", LongType(), True),
        StructField("is_fastq", BooleanType(), True)
    ])
    
    df = spark.createDataFrame(sequences_rdd.map(record_to_dict), schema)
    return df

def sample_by_number(sequences_df, number: int, seed: int = 11):
    """按数量采样"""
    total_count = sequences_df.count()
    
    if number >= total_count:
        return sequences_df
    
    # 计算采样比例，加上1.1的安全系数
    fraction = min(1.0, number / total_count * 1.1)
    
    # 进行采样
    sampled_df = sequences_df.sample(withReplacement=False, fraction=fraction, seed=seed)
    
    # 如果采样结果超过需要的数量，进行精确限制
    sampled_count = sampled_df.count()
    if sampled_count > number:
        sampled_df = sampled_df.orderBy(rand(seed)).limit(number)
    
    return sampled_df

def sample_by_proportion(sequences_df, proportion: float, seed: int = 11):
    """按比例采样"""
    return sequences_df.sample(withReplacement=False, fraction=proportion, seed=seed)

def write_sequences(sequences_df, output_path: str):
    """写入序列到文件"""
    # 格式化输出
    formatted_rdd = sequences_df.rdd.map(format_sequence)
    
    if output_path == '-':
        # 输出到stdout
        results = formatted_rdd.collect()
        for result in results:
            print(result)
    else:
        # 收集所有结果到driver
        results = formatted_rdd.collect()
        
        # 检查是否需要压缩输出
        if output_path.endswith('.gz'):
            # 输出为gzip压缩文件
            with gzip.open(output_path, 'wt') as f:
                for result in results:
                    f.write(result + '\n')
        else:
            # 输出为普通文本文件
            with open(output_path, 'w') as f:
                for result in results:
                    f.write(result + '\n')

def run_sampling_optimized(spark, input_file: str, output_file: str, number: int = 0, 
                          proportion: float = 0.0, seed: int = 11, format_type: str = 'auto',
                          quiet: bool = False, use_preprocessing: bool = True):
    """运行优化版本的采样任务"""
    if not quiet:
        print("SeqSpark - 高性能分布式生物序列采样工具（优化版）")
        print(f"输入文件: {input_file}")
        print(f"输出文件: {output_file}")
        
        if number > 0:
            print(f"采样方式: 按数量采样 ({number})")
        else:
            print(f"采样方式: 按比例采样 ({proportion})")
        
        print(f"随机种子: {seed}")
        print(f"格式: {format_type}")
        print(f"压缩文件预处理: {'启用' if use_preprocessing else '禁用'}")
    
    start_time = time.time()
    
    # 读取序列
    sequences_df = read_sequences_optimized(spark, input_file, format_type, use_preprocessing)
    
    read_time = time.time() - start_time
    if not quiet:
        total_count = sequences_df.count()
        print(f"总序列数: {total_count}")
        print(f"读取耗时: {read_time:.2f}秒")
    
    # 执行采样
    sample_start_time = time.time()
    
    if number > 0:
        sampled_df = sample_by_number(sequences_df, number, seed)
    else:
        sampled_df = sample_by_proportion(sequences_df, proportion, seed)
    
    # 缓存结果以提高性能
    sampled_df.cache()
    
    sample_time = time.time() - sample_start_time
    if not quiet:
        sampled_count = sampled_df.count()
        print(f"采样序列数: {sampled_count}")
        print(f"采样耗时: {sample_time:.2f}秒")
    
    # 写入结果
    write_start_time = time.time()
    write_sequences(sampled_df, output_file)
    
    write_time = time.time() - write_start_time
    total_time = time.time() - start_time
    
    if not quiet:
        print(f"写入耗时: {write_time:.2f}秒")
        print(f"总耗时: {total_time:.2f}秒")
        print("采样完成！")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="SeqSpark - 高性能分布式生物序列采样工具（优化版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
优化版本特性:
- 自动处理压缩文件的分区问题
- 使用RDD重新分区技术
- 支持Snappy压缩格式
- 自动优化分区数量
- 更好的性能监控

示例用法:
  # 优化处理大型压缩文件
  python seqspark_sample_optimized.py -i large.fastq.gz -n 10000 -o sampled.fastq.gz
  
  # 禁用预处理（直接处理压缩文件）
  python seqspark_sample_optimized.py -i large.fastq.gz -n 10000 -o sampled.fastq.gz --no-preprocessing
  
  # 指定分区数量
  python seqspark_sample_optimized.py -i large.fastq.gz -n 10000 -o sampled.fastq.gz --partitions 1000
        """
    )
    
    parser.add_argument('-i', '--input', required=True,
                       help='输入FASTA/FASTQ文件路径（支持.gz, .bz2, .xz压缩）')
    parser.add_argument('-o', '--output', default='-',
                       help='输出文件路径（默认：标准输出）。支持.gz压缩格式')
    parser.add_argument('-n', '--number', type=int, default=0,
                       help='按数量采样')
    parser.add_argument('-p', '--proportion', type=float, default=0.0,
                       help='按比例采样（0.0-1.0）')
    parser.add_argument('-s', '--seed', type=int, default=11,
                       help='随机种子（默认：11）')
    parser.add_argument('-f', '--format', choices=['auto', 'fasta', 'fastq'], 
                       default='auto', help='序列格式（默认：自动检测）')
    parser.add_argument('-q', '--quiet', action='store_true',
                       help='静默模式')
    parser.add_argument('--master', default='local[*]',
                       help='Spark master URL（默认：local[*]）')
    parser.add_argument('--memory', default='4g',
                       help='Spark内存配置（默认：4g）')
    parser.add_argument('--partitions', type=int, default=None,
                       help='目标分区数量（默认：自动计算）')
    parser.add_argument('--no-preprocessing', action='store_true',
                       help='禁用压缩文件预处理')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.number <= 0 and args.proportion <= 0.0:
        print("错误：必须指定 --number 或 --proportion 其中之一", file=sys.stderr)
        sys.exit(1)
    
    if args.number > 0 and args.proportion > 0.0:
        print("错误：--number 和 --proportion 不能同时指定", file=sys.stderr)
        sys.exit(1)
    
    if args.proportion > 1.0 or args.proportion < 0.0:
        print("错误：--proportion 必须在 0.0 到 1.0 之间", file=sys.stderr)
        sys.exit(1)
    
    if not Path(args.input).exists() and not args.input.startswith(('hdfs://', 's3://', 'gs://')):
        print(f"错误：输入文件不存在: {args.input}", file=sys.stderr)
        sys.exit(1)
    
    # 创建Spark会话
    spark = SparkSession\
        .builder\
        .appName("SeqSpark-Optimized")\
        .master(args.master)\
        .config("spark.driver.memory", args.memory)\
        .config("spark.executor.memory", args.memory)\
        .config("spark.sql.adaptive.enabled", "true")\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")\
        .config("spark.sql.adaptive.skewJoin.enabled", "true")\
        .config("spark.sql.shuffle.partitions", "200")\
        .config("spark.sql.files.maxPartitionBytes", "134217728")\
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")\
        .config("spark.io.compression.codec", "snappy")\
        .getOrCreate()
    
    # 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 运行采样
        run_sampling_optimized(
            spark=spark,
            input_file=args.input,
            output_file=args.output,
            number=args.number,
            proportion=args.proportion,
            seed=args.seed,
            format_type=args.format,
            quiet=args.quiet,
            use_preprocessing=not args.no_preprocessing
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 