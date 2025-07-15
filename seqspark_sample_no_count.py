#!/usr/bin/env python3
"""
SeqSpark Minimal - Enhanced with YARN Error Handling

专注于：
1. 最高性能的RDD处理
2. 最大化并行性
3. 最少的内存使用
4. 修复YARN cluster模式输出问题
"""

import argparse
import sys
import os
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

def safe_output_write(spark_context, output_rdd, output_path, overwrite=False):
    """安全的输出写入，处理YARN集群模式的常见问题"""
    try:
        # 检查是否是HDFS路径
        is_hdfs = output_path.startswith(('hdfs://', '/'))
        
        if is_hdfs:
            # HDFS路径处理
            hadoop_conf = spark_context._jsc.hadoopConfiguration()
            fs = spark_context._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            output_path_obj = spark_context._jvm.org.apache.hadoop.fs.Path(output_path)
            
            # 检查输出目录是否存在
            if fs.exists(output_path_obj):
                if overwrite:
                    print(f"输出目录已存在，正在删除: {output_path}")
                    fs.delete(output_path_obj, True)  # True表示递归删除
                else:
                    raise Exception(f"输出目录已存在: {output_path}. 使用 --overwrite 强制覆盖")
        
        # 对于大型集群，避免使用coalesce(1)以防止内存问题
        # 使用适当的分区数
        num_partitions = max(1, min(output_rdd.getNumPartitions(), 10))
        output_rdd_partitioned = output_rdd.coalesce(num_partitions)
        
        # 执行保存操作
        output_rdd_partitioned.saveAsTextFile(output_path)
        print(f"成功保存到: {output_path}")
        
    except Exception as e:
        print(f"保存失败: {str(e)}")
        print("可能的解决方案:")
        print("1. 检查输出路径权限")
        print("2. 确保输出目录不存在，或使用 --overwrite")
        print("3. 检查HDFS连接")
        raise e

def main():
    parser = argparse.ArgumentParser(description="SeqSpark Minimal Enhanced - 修复YARN问题")
    parser.add_argument('-i', '--input', required=True, help='输入FASTQ文件')
    parser.add_argument('-o', '--output', default='-', help='输出文件')
    parser.add_argument('-p', '--proportion', type=float, required=True, help='采样比例 (0.0-1.0)')
    parser.add_argument('-s', '--seed', type=int, default=11, help='随机种子')
    parser.add_argument('--overwrite', action='store_true', help='覆盖现有输出目录')
    parser.add_argument('--master', default='local[*]', help='Spark master')
    parser.add_argument('--memory', default='4g', help='内存配置')
    
    args = parser.parse_args()
    
    # 验证参数
    if not (0.0 < args.proportion <= 1.0):
        print("错误：采样比例必须在 0.0 到 1.0 之间", file=sys.stderr)
        sys.exit(1)
    
    # 创建Spark会话 - 最优配置
    spark = SparkSession.builder \
        .appName("SeqSpark-Enhanced") \
        .master(args.master) \
        .config("spark.driver.memory", args.memory) \
        .config("spark.executor.memory", args.memory) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print(f"SeqSpark Enhanced - 处理文件: {args.input}")
        print(f"采样比例: {args.proportion}")
        print(f"输出路径: {args.output}")
        
        # 1. 读取文件为RDD
        lines_rdd = spark.sparkContext.textFile(args.input)
        print(f"输入分区数: {lines_rdd.getNumPartitions()}")
        
        # 2. 解析FASTQ记录
        records_rdd = lines_rdd.mapPartitions(parse_fastq_partition)
        
        # 3. 采样
        sampled_rdd = records_rdd.sample(False, args.proportion, args.seed)
        
        # 4. 格式化输出
        output_rdd = sampled_rdd.map(format_fastq_record)
        
        # 5. 输出结果 - 使用安全的输出方法
        if args.output == '-':
            # 输出到标准输出
            results = output_rdd.collect()
            for result in results:
                print(result)
        else:
            # 使用增强的输出方法
            safe_output_write(spark.sparkContext, output_rdd, args.output, args.overwrite)
            
        print(f"采样完成！")
        
    except Exception as e:
        print(f"任务失败: {str(e)}", file=sys.stderr)
        sys.exit(1)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 