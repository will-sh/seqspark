# SeqSpark 优化指南

## 压缩文件性能问题

### 问题描述
当使用Spark处理gzip压缩文件时，会遇到以下问题：
```
Loading one large unsplittable file file:/path/to/file.gz with only one partition, because the file is compressed by unsplittable compression codec.
```

**原因：**
- gzip、bz2、xz等压缩格式不支持随机访问
- Spark无法将压缩文件分割成多个分区
- 整个文件只能在一个分区中处理，无法并行化

### 解决方案

我们提供了两种解决方案：

#### 方案1：使用优化版本脚本（推荐）
```bash
# 使用优化版本自动处理压缩文件
python seqspark_sample_optimized.py -i K562_100M_R1.fastq.gz -n 50000000 -s 100 -o output.fastq.gz
```

#### 方案2：手动预处理
```bash
# 先解压缩文件
gunzip -c K562_100M_R1.fastq.gz > K562_100M_R1.fastq

# 使用未压缩文件
python seqspark_sample.py -i K562_100M_R1.fastq -n 50000000 -s 100 -o output.fastq.gz

# 清理临时文件
rm K562_100M_R1.fastq
```

#### 方案3：使用性能优化脚本
```bash
# 使用自动化优化脚本
./optimize_performance.sh -i K562_100M_R1.fastq.gz -n 50000000 -s 100 -o output.fastq.gz
```

## 优化版本特性

### 1. 自动RDD重新分区
```python
# 读取压缩文件为RDD
rdd = spark.sparkContext.textFile(file_path)

# 重新分区到更多分区
repartitioned_rdd = rdd.repartition(target_partitions)

# 保存为可分割格式
repartitioned_rdd.saveAsTextFile(temp_output_dir,
    compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")
```

### 2. 智能分区数量计算
```python
def estimate_partitions(file_path: str, target_partition_size_mb: int = 128) -> int:
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    # 压缩文件估算解压后大小
    if is_compressed_file(file_path):
        file_size_mb *= 4
    return max(2, min(10000, int(file_size_mb / target_partition_size_mb)))
```

### 3. 临时文件管理
- 自动创建临时目录
- 使用Snappy压缩格式保存中间结果
- 程序结束后自动清理临时文件

## 使用示例

### 基本使用
```bash
# 处理100M行的压缩FASTQ文件，采样5000万条序列
python seqspark_sample_optimized.py \
    -i K562_100M_R1.fastq.gz \
    -n 50000000 \
    -s 100 \
    -o seqspark_K562_100M_R1.fastq.gz
```

### 高级参数
```bash
# 指定分区数量和内存
python seqspark_sample_optimized.py \
    -i K562_100M_R1.fastq.gz \
    -n 50000000 \
    -s 100 \
    -o seqspark_K562_100M_R1.fastq.gz \
    --partitions 2000 \
    --memory 32g \
    --master "local[16]"
```

### 禁用预处理（对比测试）
```bash
# 禁用预处理，直接处理压缩文件（会很慢）
python seqspark_sample_optimized.py \
    -i K562_100M_R1.fastq.gz \
    -n 50000000 \
    -s 100 \
    -o seqspark_K562_100M_R1.fastq.gz \
    --no-preprocessing
```

## 性能对比

| 方法 | 处理时间 | 分区数 | 内存使用 | 磁盘使用 |
|------|----------|--------|----------|----------|
| 原始方法（压缩） | 30-60分钟 | 1 | 高 | 低 |
| 优化方法 | 5-10分钟 | 自动计算 | 中 | 中等（临时文件） |
| 手动解压 | 3-8分钟 | 自动分割 | 低 | 高 |

## 输出信息解读

### 优化版本输出
```
SeqSpark - 高性能分布式生物序列采样工具（优化版）
输入文件: K562_100M_R1.fastq.gz
输出文件: seqspark_K562_100M_R1.fastq.gz
采样方式: 按数量采样 (50000000)
随机种子: 100
格式: auto
压缩文件预处理: 启用
[优化] 检测到压缩文件，开始预处理...
[优化] 目标分区数: 2500
[优化] 读取压缩文件...
[优化] 重新分区到 2500 个分区...
[优化] 保存为可分割格式...
[优化] 预处理完成，耗时: 45.23秒
总序列数: 100000000
读取耗时: 78.45秒
采样序列数: 50000000
采样耗时: 12.34秒
写入耗时: 23.56秒
总耗时: 114.35秒
采样完成！
```

### 关键性能指标
- **预处理时间**：RDD重新分区耗时
- **读取时间**：从预处理后的文件读取数据
- **采样时间**：执行采样操作
- **写入时间**：保存结果到文件
- **总耗时**：整个流程的总时间

## 故障排除

### 1. 临时空间不足
```bash
# 指定临时目录
export TMPDIR=/path/to/large/disk
python seqspark_sample_optimized.py ...
```

### 2. 内存不足
```bash
# 增加内存配置
python seqspark_sample_optimized.py ... --memory 64g
```

### 3. Snappy压缩不可用
如果Snappy压缩不可用，脚本会自动降级为无压缩格式：
```
[优化] Snappy压缩失败，使用无压缩格式: ...
```

### 4. 分区数量过多
```bash
# 手动指定较少的分区数
python seqspark_sample_optimized.py ... --partitions 500
```

## 建议配置

### 小文件（< 1GB）
```bash
--partitions 50 --memory 8g
```

### 中等文件（1-10GB）
```bash
--partitions 200 --memory 16g
```

### 大文件（10-100GB）
```bash
--partitions 1000 --memory 32g
```

### 超大文件（> 100GB）
```bash
--partitions 2000 --memory 64g --master "local[32]"
```

## 最佳实践

1. **首次运行**：使用默认设置，让系统自动优化
2. **性能调优**：根据输出调整分区数量和内存
3. **集群环境**：使用适当的master URL和资源配置
4. **监控资源**：观察CPU、内存和磁盘使用情况
5. **批处理**：对多个文件使用相同的参数配置

## 与原版本对比

| 特性 | 原版本 | 优化版本 |
|------|--------|----------|
| 压缩文件处理 | 单分区 | 自动重新分区 |
| 分区数量 | 固定 | 智能计算 |
| 临时文件管理 | 无 | 自动管理 |
| 性能监控 | 基本 | 详细 |
| 内存优化 | 一般 | 优化 |
| 错误处理 | 基本 | 增强 |

通过使用优化版本，你可以获得显著的性能提升，特别是在处理大型压缩文件时。 