#!/bin/bash

# Start containers
docker-compose up -d

echo "Waiting for Cassandra CQL port..."
MAX_WAIT=360
WAITED=0

until docker exec cassandra cqlsh cassandra 9042 -e "DESCRIBE KEYSPACES;" &> /dev/null; do
    sleep 5
    WAITED=$((WAITED+5))
    echo -n "."
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "Cassandra CQL did not become ready in time"
        exit 1
    fi
done
echo "Cassandra is ready!"



docker exec -i cassandra cqlsh cassandra 9042 -e "DESC KEYSPACES;"

docker exec -i cassandra cqlsh cassandra 9042 -f /init.cql || { echo "Cassandra init failed"; exit 1; }

chmod -R 777 ./model || { echo "Failed to set model permissions"; exit 1; }


docker exec bigdata_project_spark-master_1 sh -c "pip install scapy --break-system-packages" || { echo "pip install failed"; exit 1; }
docker exec bigdata_project_spark-master_1 sh -c "cd /scripts && python3 realtime_netflow_predictor_savedb.py" || { echo "Python script failed"; exit 1; }
#docker exec -it cassandra cqlsh cassandra 9042 -e "SELECT COUNT(*) FROM netflow.flows;" && docker exec -it bigdata_project_spark-master_1 spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 --conf spark.cassandra.connection.host=cassandra --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=cassandra /scripts/clean_and_load.py


docker exec -it cassandra cqlsh cassandra 9042 -e "SELECT * FROM netflow.flows LIMIT 20;"


#docker exec -it cassandra cqlsh cassandra 9042 -e "SELECT * FROM netflow.flows LIMIT 10;" && docker exec -it bigdata_project_spark-master_1 pip install matplotlib pandas --break-system-packages && docker exec -it bigdata_project_spark-master_1 spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 --driver-memory 4g --executor-memory 2g /scripts/train_model.py






docker exec -it bigdata_project_spark-master_1 curl -s https://google.com > /dev/null
#docker exec -it bigdata_project_spark-master_1 apt update && apt install -y hping3 && docker exec -it bigdata_project_spark-master_1 hping3 -S -p 80 --flood 8.8.8.8



docker exec -it cassandra cqlsh cassandra 9042 -e "SELECT * FROM netflow.predictions;"