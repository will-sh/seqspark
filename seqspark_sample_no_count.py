#!/usr/bin/env python3
"""
SeqSpark Minimal - 极简高性能FASTQ采样工具

专注于：
1. 最高性能的RDD处理
2. 最大化并行性
3. 最少的内存使用
4. 仅支持未压缩FASTQ文件
"""

import argparse
import sys
from pyspark.sql import SparkSession

def parse_fastq_partition(lines):
    """高效解析FASTQ分区 - 每4行一个记录"""
    lines_list = list(lines)
    records = []
    
    i = 0
    while i + 3 < len(lines_list):
        if lines_list[i].startswith('@'):
            # FASTQ record: header, seq, +, qual
            header = lines_list[i][1:].strip()
            sequence = lines_list[i + 1].strip()
            quality = lines_list[i + 3].strip()
            
            # 简单元组结构 - 最小内存开销
            records.append((header, sequence, quality))
            i += 4
        else:
            i += 1
    
    return records

def format_fastq_record(record):
    """格式化FASTQ记录输出"""
    header, sequence, quality = record
    return f"@{header}\n{sequence}\n+\n{quality}"

def main():
    parser = argparse.ArgumentParser(description="SeqSpark Minimal - 极简高性能FASTQ采样")
    parser.add_argument('-i', '--input', required=True, help='输入FASTQ文件')
    parser.add_argument('-o', '--output', default='-', help='输出文件')
    parser.add_argument('-p', '--proportion', type=float, required=True, help='采样比例 (0.0-1.0)')
    parser.add_argument('-s', '--seed', type=int, default=11, help='随机种子')
    parser.add_argument('--master', default='local[*]', help='Spark master')
    parser.add_argument('--memory', default='4g', help='内存配置')
    
    args = parser.parse_args()
    
    # 验证参数
    if not (0.0 < args.proportion <= 1.0):
        print("错误：采样比例必须在 0.0 到 1.0 之间", file=sys.stderr)
        sys.exit(1)
    
    # 创建Spark会话 - 最优配置
    spark = SparkSession.builder \
        .appName("SeqSpark-Minimal") \
        .master(args.master) \
        .config("spark.driver.memory", args.memory) \
        .config("spark.executor.memory", args.memory) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print(f"SeqSpark Minimal - 处理文件: {args.input}")
        print(f"采样比例: {args.proportion}")
        
        # 1. 读取文件为RDD - 让Spark自动分区以获得最大并行性
        lines_rdd = spark.sparkContext.textFile(args.input)
        
        # 2. 解析FASTQ记录 - 使用mapPartitions提高效率
        records_rdd = lines_rdd.mapPartitions(parse_fastq_partition)
        
        # 3. 采样 - 直接使用RDD.sample，最高效的采样方法
        sampled_rdd = records_rdd.sample(False, args.proportion, args.seed)
        
        # 4. 格式化输出
        output_rdd = sampled_rdd.map(format_fastq_record)
        
        # 5. 输出结果
        if args.output == '-':
            # 输出到标准输出
            results = output_rdd.collect()
            for result in results:
                print(result)
        else:
            # 保存到文件 - 使用coalesce减少输出文件数量
            output_rdd.coalesce(1).saveAsTextFile(args.output)
            
        print(f"采样完成！", file=sys.stderr)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 