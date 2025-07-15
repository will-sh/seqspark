#!/usr/bin/env python3
"""
SeqSpark - 高性能分布式生物序列采样工具（无Count优化版）

主要优化：
1. 去除不必要的count()操作
2. 智能采样策略，不依赖总数统计
3. 可选的快速估算模式
4. 异步count选项
"""

from __future__ import print_function
import argparse
import sys
import os
import gzip
import bz2
import lzma
import threading
import time
from typing import Iterator, Optional, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, rand
    from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
except ImportError:
    print("错误：需要安装PySpark。请运行：pip install pyspark", file=sys.stderr)
    sys.exit(1)

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
    """格式化序列输出的独立函数"""
    if row.is_fastq:
        return f"@{row.name}\n{row.sequence}\n+\n{row.quality or ''}"
    else:
        return f">{row.name}\n{row.sequence}"

@dataclass
class SequenceRecord:
    """序列记录数据结构"""
    id: str
    name: str
    sequence: str
    quality: Optional[str] = None
    line_number: int = 0
    is_fastq: bool = False
    
    def format_output(self) -> str:
        """格式化输出"""
        if self.is_fastq:
            return f"@{self.name}\n{self.sequence}\n+\n{self.quality or ''}"
        else:
            return f">{self.name}\n{self.sequence}"

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
        opener = get_file_opener(file_path)
        with opener(file_path, 'rt') as f:
            lines = [f.readline().strip() for _ in range(10)]
        
        fasta_count = sum(1 for line in lines if line.startswith('>'))
        fastq_count = sum(1 for line in lines if line.startswith('@'))
        
        return 'fastq' if fastq_count > fasta_count else 'fasta'
    except Exception as e:
        print(f"警告：格式检测失败: {e}。默认使用FASTA格式。", file=sys.stderr)
        return 'fasta'

def estimate_sequence_count(file_path: str, format_type: str) -> int:
    """快速估算序列数量（基于文件大小）"""
    try:
        file_size = os.path.getsize(file_path)
        if format_type == 'fastq':
            # FASTQ: 平均每序列约300-400字节
            estimated_count = file_size // 350
        else:
            # FASTA: 平均每序列约200字节
            estimated_count = file_size // 200
        return max(estimated_count, 1)
    except:
        return 1000000  # 默认估算值

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
            if current_header and current_id:
                yield SequenceRecord(
                    id=current_id,
                    name=current_header,
                    sequence=''.join(current_seq),
                    quality=None,
                    line_number=line_number,
                    is_fastq=False
                )
            
            current_header = line[1:]
            current_id = current_header.split()[0]
            current_seq = []
        elif current_header:
            current_seq.append(line)
    
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

def read_sequences(spark, file_path: str, format_type: str = 'auto'):
    """读取序列文件"""
    if format_type == 'auto':
        format_type = detect_format(file_path)
    
    text_rdd = spark.sparkContext.textFile(file_path)
    
    if format_type == 'fastq':
        sequences_rdd = text_rdd.mapPartitions(parse_fastq_partition)
    else:
        sequences_rdd = text_rdd.mapPartitions(parse_fasta_partition)
    
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

def smart_sample_by_number(sequences_df, number: int, seed: int = 11, 
                          estimated_total: Optional[int] = None):
    """智能按数量采样（不依赖count）"""
    if estimated_total:
        # 使用估算值计算采样比例
        if number >= estimated_total:
            return sequences_df
        
        # 计算采样比例，加上安全系数
        fraction = min(1.0, number / estimated_total * 1.2)
    else:
        # 如果没有估算值，使用保守的采样策略
        # 假设需要采样的数量相对较小，使用较大的安全系数
        fraction = min(1.0, number / 100000 * 2.0) if number < 100000 else 0.8
    
    # 进行采样
    sampled_df = sequences_df.sample(withReplacement=False, fraction=fraction, seed=seed)
    
    # 如果采样过多，限制数量（这个count是对小数据集的，很快）
    final_df = sampled_df.limit(number)
    
    return final_df

def sample_by_proportion(sequences_df, proportion: float, seed: int = 11):
    """按比例采样（无需count）"""
    return sequences_df.sample(withReplacement=False, fraction=proportion, seed=seed)

def async_count(sequences_df, result_container: List):
    """异步执行count操作"""
    try:
        count = sequences_df.count()
        result_container.append(count)
    except Exception as e:
        result_container.append(f"Count错误: {e}")

def write_sequences(sequences_df, output_path: str):
    """写入序列到文件"""
    formatted_rdd = sequences_df.rdd.map(format_sequence)
    
    if output_path == '-':
        results = formatted_rdd.collect()
        for result in results:
            print(result)
    else:
        results = formatted_rdd.collect()
        
        if output_path.endswith('.gz'):
            with gzip.open(output_path, 'wt') as f:
                for result in results:
                    f.write(result + '\n')
        else:
            with open(output_path, 'w') as f:
                for result in results:
                    f.write(result + '\n')

def run_sampling_optimized(spark, input_file: str, output_file: str, number: int = 0, 
                          proportion: float = 0.0, seed: int = 11, format_type: str = 'auto',
                          quiet: bool = False, skip_count: bool = False, 
                          async_count_enabled: bool = False):
    """运行优化的采样任务"""
    start_time = time.time()
    
    if not quiet:
        print("SeqSpark - 高性能采样工具（无Count优化版）")
        print(f"输入文件: {input_file}")
        print(f"输出文件: {output_file}")
        
        if number > 0:
            print(f"采样方式: 按数量采样 ({number})")
        else:
            print(f"采样方式: 按比例采样 ({proportion})")
        
        print(f"随机种子: {seed}")
        print(f"格式: {format_type}")
    
    # 读取序列
    read_start = time.time()
    sequences_df = read_sequences(spark, input_file, format_type)
    read_time = time.time() - read_start
    
    # 快速估算序列数量（可选）
    estimated_total = None
    if number > 0 and not skip_count:
        estimated_total = estimate_sequence_count(input_file, format_type)
        if not quiet:
            print(f"估算序列数: ~{estimated_total:,}")
    
    # 可选的异步精确count
    count_result = []
    count_thread = None
    if async_count_enabled and not skip_count:
        if not quiet:
            print("启动异步精确计数...")
        count_thread = threading.Thread(target=async_count, args=(sequences_df, count_result))
        count_thread.start()
    
    # 执行采样（核心优化：不等待count完成）
    sample_start = time.time()
    if number > 0:
        sampled_df = smart_sample_by_number(sequences_df, number, seed, estimated_total)
    else:
        sampled_df = sample_by_proportion(sequences_df, proportion, seed)
    
    # 缓存结果
    sampled_df.cache()
    sample_time = time.time() - sample_start
    
    # 快速验证采样结果（仅对小数据集count）
    if not quiet:
        verify_start = time.time()
        sampled_count = sampled_df.count()  # 这个count很快，因为是采样后的小数据集
        verify_time = time.time() - verify_start
        print(f"实际采样数: {sampled_count:,}")
        print(f"验证耗时: {verify_time:.2f}秒")
    
    # 写入结果
    write_start = time.time()
    write_sequences(sampled_df, output_file)
    write_time = time.time() - write_start
    
    # 显示异步count结果（如果启用）
    if count_thread:
        if not quiet:
            print("等待精确计数完成...")
        count_thread.join()
        if count_result and not quiet:
            print(f"精确序列数: {count_result[0]:,}")
    
    total_time = time.time() - start_time
    
    if not quiet:
        print(f"\n性能统计:")
        print(f"读取耗时: {read_time:.2f}秒")
        print(f"采样耗时: {sample_time:.2f}秒")
        print(f"写入耗时: {write_time:.2f}秒")
        print(f"总耗时: {total_time:.2f}秒")
        print("采样完成！")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="SeqSpark - 无Count优化版本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
优化特性:
  --skip-count     完全跳过count操作（最快）
  --async-count    异步执行精确count（平衡性能和信息）
  
示例用法:
  # 最快模式：跳过所有count
  python seqspark_sample_no_count.py -i large.fastq.gz -n 10000 -o sampled.fastq.gz --skip-count
  
  # 平衡模式：异步count + 快速估算
  python seqspark_sample_no_count.py -i large.fastq.gz -n 10000 -o sampled.fastq.gz --async-count
  
  # 静默快速模式
  python seqspark_sample_no_count.py -i large.fastq.gz -n 10000 -o sampled.fastq.gz -q --skip-count
        """
    )
    
    parser.add_argument('-i', '--input', required=True,
                       help='输入FASTA/FASTQ文件路径')
    parser.add_argument('-o', '--output', default='-',
                       help='输出文件路径（默认：标准输出）')
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
    parser.add_argument('--skip-count', action='store_true',
                       help='跳过count操作以获得最大性能')
    parser.add_argument('--async-count', action='store_true',
                       help='异步执行精确count（平衡性能和信息）')
    
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
        .appName("SeqSpark-NoCount")\
        .master(args.master)\
        .config("spark.driver.memory", args.memory)\
        .config("spark.executor.memory", args.memory)\
        .config("spark.sql.adaptive.enabled", "true")\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")\
        .config("spark.sql.adaptive.skewJoin.enabled", "true")\
        .config("spark.sql.shuffle.partitions", "100")\
        .config("spark.sql.files.maxPartitionBytes", "134217728")\
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        run_sampling_optimized(
            spark=spark,
            input_file=args.input,
            output_file=args.output,
            number=args.number,
            proportion=args.proportion,
            seed=args.seed,
            format_type=args.format,
            quiet=args.quiet,
            skip_count=args.skip_count,
            async_count_enabled=args.async_count
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 