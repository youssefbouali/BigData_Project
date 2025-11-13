#!/bin/bash
echo "Starting Spark Worker..."
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$@"  
