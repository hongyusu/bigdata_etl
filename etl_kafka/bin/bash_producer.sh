
# Run producer
../../confluent-3.0.0/bin/kafka-console-producer --topic loguser --broker-list localhost:9092 < ../etl_spark/data/log_user &
../../confluent-3.0.0/bin/kafka-console-producer --topic logaction --broker-list localhost:9092 < ../etl_spark/data/log_action 

# Run consumer
#../../spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class etl_kafka.KafkaETLMain build/libs/etl_kafka-all.jar --bootstrap-url 10.0.1.3:9092
