#!/bin/bash
echo "Starting Spark Worker..."
/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$@"  
