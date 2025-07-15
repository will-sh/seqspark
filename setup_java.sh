#!/bin/bash

# SeqSpark Java 环境设置脚本
# 此脚本帮助解决Java版本兼容性问题

echo "SeqSpark Java 环境设置"
echo "===================="

# 检查当前Java版本
echo "检查当前Java版本..."
java -version 2>&1 | head -n 3

# 获取系统信息
if [ -f /etc/redhat-release ]; then
    OS="centos"
    echo "检测到 CentOS/RHEL 系统"
elif [ -f /etc/debian_version ]; then
    OS="ubuntu"
    echo "检测到 Ubuntu/Debian 系统"
else
    OS="unknown"
    echo "未知操作系统"
fi

echo ""
echo "解决方案选择："
echo "1. 安装Java 17 (推荐)"
echo "2. 使用Spark 3.3.2版本"
echo "3. 手动设置Java环境变量"
echo ""

read -p "请选择解决方案 (1-3): " choice

case $choice in
    1)
        echo "正在安装Java 17..."
        if [ "$OS" = "centos" ]; then
            sudo yum install -y java-17-openjdk java-17-openjdk-devel
            JAVA_HOME="/usr/lib/jvm/java-17-openjdk"
        elif [ "$OS" = "ubuntu" ]; then
            sudo apt-get update
            sudo apt-get install -y openjdk-17-jdk
            JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
        else
            echo "请手动安装Java 17"
            exit 1
        fi
        
        # 设置环境变量
        echo "设置环境变量..."
        export JAVA_HOME=$JAVA_HOME
        export PATH=$JAVA_HOME/bin:$PATH
        
        # 添加到bashrc
        echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
        echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
        
        echo "Java 17 安装完成！"
        java -version
        ;;
        
         2)
        echo "已更新requirements.txt使用Spark 3.3.2版本"
        echo "请重新安装依赖："
        echo "pip uninstall -y pyspark"
        echo "pip install -r requirements.txt"
        ;;
        
    3)
        echo "请手动设置以下环境变量："
        echo "export JAVA_HOME=/path/to/your/java17"
        echo "export PATH=\$JAVA_HOME/bin:\$PATH"
        echo ""
        echo "或者添加到 ~/.bashrc 文件中"
        ;;
        
    *)
        echo "无效选择"
        exit 1
        ;;
esac

echo ""
echo "设置完成！请重新运行 SeqSpark 命令。" 