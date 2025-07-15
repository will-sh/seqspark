#!/bin/bash
"""
SeqSpark YARN Cluster Mode Deployment Script

Usage:
  ./run_yarn_cluster.sh [OPTIONS] -i INPUT_FILE -o OUTPUT_FILE -p PROPORTION

Examples:
  # Basic cluster run
  ./run_yarn_cluster.sh -i /hdfs/data/sample.fastq -o /hdfs/output/result.fastq -p 0.1

  # Large-scale production run
  ./run_yarn_cluster.sh --large -i /hdfs/data/60GB_file.fastq -o /hdfs/output/sampled.fastq -p 0.01

  # Custom resource allocation
  ./run_yarn_cluster.sh --executors 50 --memory 64g -i input.fastq -o output.fastq -p 0.05
"""

set -e  # Exit on any error

# Default configurations
SCRIPT_NAME="seqspark_sample_no_count.py"
APP_NAME="SeqSpark-Cluster"
DRIVER_MEMORY="8g"
EXECUTOR_MEMORY="16g"
EXECUTOR_CORES="4"
NUM_EXECUTORS="10"
PYTHON_ENV="seqspark_env.zip"

# Parse command line arguments
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
    -p|--proportion)
      PROPORTION="$2"
      shift 2
      ;;
    -s|--seed)
      SEED="$2"
      shift 2
      ;;
    --large)
      # Large-scale configuration
      DRIVER_MEMORY="16g"
      EXECUTOR_MEMORY="32g"
      EXECUTOR_CORES="8"
      NUM_EXECUTORS="30"
      APP_NAME="SeqSpark-LargeScale"
      shift
      ;;
    --executors)
      NUM_EXECUTORS="$2"
      shift 2
      ;;
    --memory)
      EXECUTOR_MEMORY="$2"
      shift 2
      ;;
    --cores)
      EXECUTOR_CORES="$2"
      shift 2
      ;;
    --name)
      APP_NAME="$2"
      shift 2
      ;;
    -h|--help)
      echo "SeqSpark YARN Cluster Mode Deployment"
      echo "Required arguments:"
      echo "  -i, --input     Input FASTQ file (HDFS path)"
      echo "  -o, --output    Output file (HDFS path)"
      echo "  -p, --proportion Sampling proportion (0.0-1.0)"
      echo ""
      echo "Optional arguments:"
      echo "  -s, --seed      Random seed (default: 11)"
      echo "  --large         Use large-scale configuration"
      echo "  --executors     Number of executors"
      echo "  --memory        Executor memory"
      echo "  --cores         Executor cores"
      echo "  --name          Application name"
      exit 0
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

# Validate required arguments
if [ -z "$INPUT_FILE" ] || [ -z "$OUTPUT_FILE" ] || [ -z "$PROPORTION" ]; then
    echo "Error: Missing required arguments"
    echo "Use --help for usage information"
    exit 1
fi

# Set default seed if not provided
SEED=${SEED:-11}

echo "=== SeqSpark YARN Cluster Deployment ==="
echo "Input file: $INPUT_FILE"
echo "Output file: $OUTPUT_FILE"
echo "Sampling proportion: $PROPORTION"
echo "Random seed: $SEED"
echo "Executors: $NUM_EXECUTORS"
echo "Executor memory: $EXECUTOR_MEMORY"
echo "Executor cores: $EXECUTOR_CORES"
echo "Driver memory: $DRIVER_MEMORY"
echo ""

# Check if required files exist
if [ ! -f "$SCRIPT_NAME" ]; then
    echo "Error: $SCRIPT_NAME not found in current directory"
    exit 1
fi

if [ ! -f "$PYTHON_ENV" ]; then
    echo "Warning: $PYTHON_ENV not found. Creating Python environment..."
    # Create virtual environment
    python -m venv seqspark_env
    source seqspark_env/bin/activate
    pip install pyspark>=3.3.2
    deactivate
    zip -r seqspark_env.zip seqspark_env/
    echo "Python environment created and packaged."
fi

# Check YARN cluster status
echo "Checking YARN cluster status..."
yarn node -list > /dev/null 2>&1 || {
    echo "Error: Cannot connect to YARN cluster"
    echo "Please ensure:"
    echo "1. YARN services are running"
    echo "2. HADOOP_CONF_DIR is set correctly"
    echo "3. You have access to the cluster"
    exit 1
}

echo "YARN cluster is accessible. Submitting job..."

# Submit Spark job to YARN cluster
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "$APP_NAME" \
  \
  --driver-memory "$DRIVER_MEMORY" \
  --driver-cores 4 \
  --executor-memory "$EXECUTOR_MEMORY" \
  --executor-cores "$EXECUTOR_CORES" \
  --num-executors "$NUM_EXECUTORS" \
  \
  --files "$SCRIPT_NAME" \
  --py-files "$PYTHON_ENV" \
  \
  --conf spark.pyspark.python=./seqspark_env/bin/python \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./seqspark_env/bin/python \
  \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.files.maxPartitionBytes=268435456 \
  --conf spark.sql.shuffle.partitions=400 \
  \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.yarn.maxAppAttempts=2 \
  \
  "$SCRIPT_NAME" \
  -i "$INPUT_FILE" \
  -o "$OUTPUT_FILE" \
  -p "$PROPORTION" \
  -s "$SEED"

echo ""
echo "=== Job Submission Complete ==="
echo "Monitor your job at: http://$(yarn node -list | head -1 | awk '{print $1}' | cut -d: -f1):8088"
echo "Check application logs with: yarn logs -applicationId <app_id>" 