#!/usr/bin/env python3
"""
SeqSpark Workaround - 完全绕过HDFS写入问题的版本
"""

import argparse
import sys
import os
import time
from pyspark.sql import SparkSession

def parse_fastq_partition(lines):
    lines_list = list(lines)
    records = []
    i = 0
    while i + 3 < len(lines_list):
        if lines_list[i].startswith('@'):
            header = lines_list[i][1:].strip()
            sequence = lines_list[i + 1].strip()
            quality = lines_list[i + 3].strip()
            records.append((header, sequence, quality))
            i += 4
        else:
            i += 1
    return records

def format_fastq_record(record):
    header, sequence, quality = record
    return f"@{header}\n{sequence}\n+\n{quality}"

def main():
    parser = argparse.ArgumentParser(description="SeqSpark Workaround - 绕过HDFS问题")
    parser.add_argument('-i', '--input', required=True, help='输入FASTQ文件')
    parser.add_argument('-o', '--output', default='-', help='输出文件')
    parser.add_argument('-p', '--proportion', type=float, required=True, help='采样比例')
    parser.add_argument('-s', '--seed', type=int, default=11, help='随机种子')
    parser.add_argument('--master', default='local[*]', help='Spark master')
    parser.add_argument('--memory', default='4g', help='内存配置')
    
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("SeqSpark-Workaround") \
        .master(args.master) \
        .config("spark.driver.memory", args.memory) \
        .config("spark.executor.memory", args.memory) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print(f"🚀 SeqSpark Workaround - 处理文件: {args.input}")
        print(f"📊 采样比例: {args.proportion}")
        
        # 处理数据
        lines_rdd = spark.sparkContext.textFile(args.input)
        records_rdd = lines_rdd.mapPartitions(parse_fastq_partition)
        sampled_rdd = records_rdd.sample(False, args.proportion, args.seed)
        output_rdd = sampled_rdd.map(format_fastq_record)
        
        # 使用collect()获取结果
        print("📦 使用collect()收集结果...")
        results = output_rdd.collect()
        print(f"✅ 成功收集 {len(results)} 条采样记录")
        
        # 输出处理
        if args.output == '-':
            for result in results:
                print(result)
        else:
            # 生成本地文件名
            if args.output.startswith(('hdfs://', '/user/')):
                local_file = f"/tmp/seqspark_workaround_{int(time.time())}.fastq"
                print(f"⚠️  HDFS路径检测到，改为本地文件: {local_file}")
            else:
                local_file = args.output
            
            # 写入本地文件
            with open(local_file, 'w') as f:
                for result in results:
                    f.write(result + '\n')
            
            print(f"✅ 成功写入: {local_file}")
            
            # 如果是HDFS路径，提供复制命令
            if args.output.startswith(('hdfs://', '/user/')):
                print(f"📋 手动复制到HDFS命令:")
                print(f"   hdfs dfs -put {local_file} {args.output}")
                print(f"   hdfs dfs -rm {local_file}  # 清理本地文件")
        
        print(f"🎉 任务完成！")
        
    except Exception as e:
        print(f"💥 任务失败: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 