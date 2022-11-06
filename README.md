# Inverted index big data
Sample Hadoop & Spark implementations to build inverted index

## Table of contents
- [Installation guide](#installation-guide)
- [Using source implementations](#using-source-implementations)
  - [Hadoop](#hadoop) 
  - [Spark](#spark) 
- [Requirements](#requirements)

## Installation guide
- Hadoop -> [link](https://www.digitalocean.com/community/tutorials/how-to-install-hadoop-in-stand-alone-mode-on-ubuntu-20-04)
- Spark -> [link](https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/)

## Using source implementations
Compile `.jar` file using `gradle`:
```shell
gradle build
```

### Hadoop
```shell
hadoop jar inverted-index-big-data-v0.0.1.jar \
    com.big.data.hadoop.InvertedIndexRunner \
    ~/path-to-input-directory \
    ~/path-to-output-directory 
```

### Spark
```shell
spark-submit --class com.big.data.spark.InvertedIndexDriver \
    inverted-index-big-data-v0.0.1.jar \
    ~/path-to-input-directory \
    ~/path-to-output-directory 
```

***Make sure environment variables are set correctly for both of them*** e.g.:
```shell
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

## Requirements
- `gradle 7.4`
- `java 17.0.2`
- `hadoop 3.3.4`
- `spark 3.3.1`
