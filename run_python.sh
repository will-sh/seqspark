#!/bin/bash

# SeqSpark Python Runner Script

set -e

# Default values
MASTER=${SPARK_MASTER:-local[*]}
DRIVER_MEMORY=${DRIVER_MEMORY:-4g}
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-4g}
EXECUTOR_CORES=${EXECUTOR_CORES:-2}
PYTHON_VERSION=${PYTHON_VERSION:-python3}

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS] -- [PYTHON_APP_OPTIONS]

Options:
    -h, --help          Show this help message
    -m, --master        Spark master URL (default: local[*])
    -d, --driver-memory Driver memory (default: 4g)
    -e, --executor-memory Executor memory (default: 4g)
    -c, --executor-cores Executor cores (default: 2)
    -p, --python        Python executable (default: python3)
    -i, --install-deps  Install dependencies before running
    -v, --verbose       Verbose output

Examples:
    # Sample 10000 sequences from a large FASTQ file
    $0 -i -- -i /path/to/large.fastq.gz -n 10000 -o sampled.fastq

    # Sample 10% of sequences with specific seed
    $0 -- -i /path/to/large.fastq.gz -p 0.1 -s 42 -o sampled.fastq

    # Run on Spark cluster
    $0 -m spark://master:7077 -d 8g -e 6g -- -i hdfs://path/to/large.fastq.gz -n 100000

    # Install dependencies and run
    $0 -i -- -i large.fastq.gz -n 5000 -o sampled.fastq

EOF
}

# Parse command line arguments
INSTALL_DEPS=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -m|--master)
            MASTER="$2"
            shift 2
            ;;
        -d|--driver-memory)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        -e|--executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        -c|--executor-cores)
            EXECUTOR_CORES="$2"
            shift 2
            ;;
        -p|--python)
            PYTHON_VERSION="$2"
            shift 2
            ;;
        -i|--install-deps)
            INSTALL_DEPS=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Remaining arguments are passed to the Python application
APP_ARGS="$@"

# Check prerequisites
if ! command_exists "$PYTHON_VERSION"; then
    print_error "Python not found: $PYTHON_VERSION. Please install Python 3.7 or higher."
    exit 1
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Install dependencies if requested
if [ "$INSTALL_DEPS" = true ]; then
    print_info "Installing dependencies..."
    if [ "$VERBOSE" = true ]; then
        "$PYTHON_VERSION" -m pip install -r requirements.txt
    else
        "$PYTHON_VERSION" -m pip install -r requirements.txt > /dev/null 2>&1
    fi
    
    if [ $? -eq 0 ]; then
        print_info "Dependencies installed successfully!"
    else
        print_error "Failed to install dependencies!"
        exit 1
    fi
fi

# Check if PySpark is available
if ! "$PYTHON_VERSION" -c "import pyspark" 2>/dev/null; then
    print_warn "PySpark not found. Installing..."
    if [ "$VERBOSE" = true ]; then
        "$PYTHON_VERSION" -m pip install pyspark
    else
        "$PYTHON_VERSION" -m pip install pyspark > /dev/null 2>&1
    fi
    
    if [ $? -eq 0 ]; then
        print_info "PySpark installed successfully!"
    else
        print_error "Failed to install PySpark!"
        exit 1
    fi
fi

# Check if script exists
SCRIPT_FILE="$SCRIPT_DIR/seqspark_sample.py"
if [ ! -f "$SCRIPT_FILE" ]; then
    print_error "Script file not found: $SCRIPT_FILE"
    exit 1
fi

# Set environment variables for Spark
export PYSPARK_PYTHON="$PYTHON_VERSION"
export PYSPARK_DRIVER_PYTHON="$PYTHON_VERSION"

# Print configuration
print_info "Configuration:"
echo "  Spark Master: $MASTER"
echo "  Driver Memory: $DRIVER_MEMORY"
echo "  Executor Memory: $EXECUTOR_MEMORY"
echo "  Executor Cores: $EXECUTOR_CORES"
echo "  Python Version: $PYTHON_VERSION"
echo "  Script File: $SCRIPT_FILE"
echo "  App Args: $APP_ARGS"
echo

# Set Spark configuration as environment variables
export SPARK_MASTER="$MASTER"
export SPARK_DRIVER_MEMORY="$DRIVER_MEMORY"
export SPARK_EXECUTOR_MEMORY="$EXECUTOR_MEMORY"
export SPARK_EXECUTOR_CORES="$EXECUTOR_CORES"

# Run Python application
print_info "Starting PySpark application..."

if [ "$VERBOSE" = true ]; then
    set -x
fi

# Run with spark-submit if available, otherwise use python directly
if command_exists spark-submit; then
    spark-submit \
        --master "$MASTER" \
        --driver-memory "$DRIVER_MEMORY" \
        --executor-memory "$EXECUTOR_MEMORY" \
        --executor-cores "$EXECUTOR_CORES" \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
        --conf "spark.sql.adaptive.skewJoin.enabled=true" \
        --conf "spark.sql.shuffle.partitions=200" \
        --conf "spark.sql.files.maxPartitionBytes=134217728" \
        --conf "spark.sql.execution.arrow.maxRecordsPerBatch=10000" \
        "$SCRIPT_FILE" \
        --master "$MASTER" \
        --memory "$DRIVER_MEMORY" \
        $APP_ARGS
else
    "$PYTHON_VERSION" "$SCRIPT_FILE" \
        --master "$MASTER" \
        --memory "$DRIVER_MEMORY" \
        $APP_ARGS
fi

if [ $? -eq 0 ]; then
    print_info "Application completed successfully!"
else
    print_error "Application failed!"
    exit 1
fi 