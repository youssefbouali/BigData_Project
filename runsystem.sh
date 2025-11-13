# 1. build and run
docker-compose up -d --build

# 2. load data
docker-compose exec spark-submit python /scripts/clean_and_load.py

# 3. model training
docker-compose exec spark-submit python /scripts/train_model.py

# 4. run Streaming (get from kafka)
docker-compose exec spark-submit python /scripts/streaming_predict.py