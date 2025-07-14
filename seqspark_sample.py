#!/usr/bin/env python3
"""
SeqSpark - 高性能分布式生物序列采样工具

这个工具使用Apache Spark来处理大型FASTQ/FASTA文件的采样，
相比原始的seqkit sample命令，在处理60GB+的文件时性能显著提升。

主要特性：
1. 分布式处理大文件
2. 支持FASTA和FASTQ格式
3. 支持gzip、bzip2、xz压缩格式
4. 按数量或比例采样
5. 可重现的随机采样
6. 内存高效处理
"""

import argparse
import sys
import os
import random
import gzip
import bz2
import lzma
import re
from typing import Iterator, Tuple, Optional, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, rand, monotonically_increasing_id, lit
    from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
    from pyspark import SparkContext, SparkConf
    from pyspark.broadcast import Broadcast
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

class SeqSparkSample:
    """SeqSpark 主类"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.sc = spark_session.sparkContext
        
    def detect_format(self, file_path: str) -> str:
        """检测文件格式"""
        try:
            # 读取前几行来检测格式
            opener = self._get_file_opener(file_path)
            with opener(file_path, 'rt') as f:
                lines = [f.readline().strip() for _ in range(10)]
            
            fasta_count = sum(1 for line in lines if line.startswith('>'))
            fastq_count = sum(1 for line in lines if line.startswith('@'))
            
            return 'fastq' if fastq_count > fasta_count else 'fasta'
        except Exception as e:
            print(f"警告：格式检测失败: {e}。默认使用FASTA格式。", file=sys.stderr)
            return 'fasta'
    
    def _get_file_opener(self, file_path: str):
        """根据文件扩展名选择合适的文件打开器"""
        if file_path.endswith('.gz'):
            return gzip.open
        elif file_path.endswith('.bz2'):
            return bz2.open
        elif file_path.endswith('.xz'):
            return lzma.open
        else:
            return open
    
    @staticmethod
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
    
    @staticmethod
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
    
    def read_sequences(self, file_path: str, format_type: str = 'auto') -> DataFrame:
        """读取序列文件"""
        # 检测格式
        if format_type == 'auto':
            format_type = self.detect_format(file_path)
        
        # 读取文件为RDD
        if file_path.endswith('.gz') or file_path.endswith('.bz2') or file_path.endswith('.xz'):
            # 对于压缩文件，直接读取
            text_rdd = self.sc.textFile(file_path)
        else:
            text_rdd = self.sc.textFile(file_path)
        
        # 解析序列
        if format_type == 'fastq':
            sequences_rdd = text_rdd.mapPartitions(SeqSparkSample.parse_fastq_partition)
        else:
            sequences_rdd = text_rdd.mapPartitions(SeqSparkSample.parse_fasta_partition)
        
        # 转换为DataFrame
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("sequence", StringType(), True),
            StructField("quality", StringType(), True),
            StructField("line_number", LongType(), True),
            StructField("is_fastq", BooleanType(), True)
        ])
        
        df = self.spark.createDataFrame(sequences_rdd.map(record_to_dict), schema)
        return df
    
    def sample_by_number(self, sequences_df: DataFrame, number: int, seed: int = 11) -> DataFrame:
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
    
    def sample_by_proportion(self, sequences_df: DataFrame, proportion: float, seed: int = 11) -> DataFrame:
        """按比例采样"""
        return sequences_df.sample(withReplacement=False, fraction=proportion, seed=seed)
    
    def write_sequences(self, sequences_df: DataFrame, output_path: str):
        """写入序列到文件"""
        # 格式化输出
        formatted_rdd = sequences_df.rdd.map(format_sequence)
        
        if output_path == '-':
            # 输出到stdout
            results = formatted_rdd.collect()
            for result in results:
                print(result)
        else:
            # 输出到文件
            formatted_rdd.coalesce(1).saveAsTextFile(output_path)
    
    def run_sampling(self, input_file: str, output_file: str, number: int = 0, 
                    proportion: float = 0.0, seed: int = 11, format_type: str = 'auto',
                    quiet: bool = False) -> None:
        """运行采样任务"""
        if not quiet:
            print(f"SeqSpark - 高性能分布式生物序列采样工具")
            print(f"输入文件: {input_file}")
            print(f"输出文件: {output_file}")
            
            if number > 0:
                print(f"采样方式: 按数量采样 ({number})")
            else:
                print(f"采样方式: 按比例采样 ({proportion})")
            
            print(f"随机种子: {seed}")
            print(f"格式: {format_type}")
        
        # 读取序列
        sequences_df = self.read_sequences(input_file, format_type)
        
        if not quiet:
            total_count = sequences_df.count()
            print(f"总序列数: {total_count}")
        
        # 执行采样
        if number > 0:
            sampled_df = self.sample_by_number(sequences_df, number, seed)
        else:
            sampled_df = self.sample_by_proportion(sequences_df, proportion, seed)
        
        # 缓存结果以提高性能
        sampled_df.cache()
        
        if not quiet:
            sampled_count = sampled_df.count()
            print(f"采样序列数: {sampled_count}")
        
        # 写入结果
        self.write_sequences(sampled_df, output_file)
        
        if not quiet:
            print("采样完成！")

def create_spark_session(app_name: str = "SeqSpark", 
                        master: str = "local[*]",
                        memory: str = "4g") -> SparkSession:
    """创建Spark会话"""
    conf = SparkConf().setAppName(app_name)
    
    if master:
        conf.setMaster(master)
    
    conf.set("spark.driver.memory", memory)
    conf.set("spark.executor.memory", memory)
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.shuffle.partitions", "200")
    conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
    conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="SeqSpark - 高性能分布式生物序列采样工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 按数量采样
  python seqspark_sample.py -i large.fastq.gz -n 10000 -o sampled.fastq
  
  # 按比例采样
  python seqspark_sample.py -i large.fastq.gz -p 0.1 -o sampled.fastq
  
  # 指定随机种子
  python seqspark_sample.py -i large.fastq.gz -n 5000 -s 42 -o sampled.fastq
  
  # 在集群上运行
  python seqspark_sample.py -i hdfs://path/to/large.fastq.gz -n 100000 \\
    --master spark://master:7077 --memory 8g
        """
    )
    
    parser.add_argument('-i', '--input', required=True,
                       help='输入FASTA/FASTQ文件路径（支持.gz, .bz2, .xz压缩）')
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
    spark = create_spark_session(
        app_name="SeqSpark",
        master=args.master,
        memory=args.memory
    )
    
    try:
        # 运行采样
        sampler = SeqSparkSample(spark)
        sampler.run_sampling(
            input_file=args.input,
            output_file=args.output,
            number=args.number,
            proportion=args.proportion,
            seed=args.seed,
            format_type=args.format,
            quiet=args.quiet
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 