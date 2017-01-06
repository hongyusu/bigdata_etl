

# Install Kafka

1. Install brew
   ```
   /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
   ```
1. Install Kafka and Zookeeper
   ```
   brew install kafka
   ```
1. Install gradle
   ```
   brew install gradle
   ```

# Kafka CLI operations

1. kafka won't work without zookeep, so start zookeeper
   ```
   zkserver start
   ```
1. start kafka server
   ```
   kafka-server-start /usr/local/etc/kafka/server.properties
   ```
1. create a topic, _zookeeper URL_, _partition_, and _replication factor_ need to be given as input parameters 
   ```
   kafka-topics --zookeeper localhost:2181 --create --topic test --partition 1 --replication-factor 1
   ```
1. start a kafka producer, _broker server url_ need to be specified
   ```
   kafka-console-producer --topic test --broker-list localhost:9092
   ```
   or send to stream content in file
   ```
   kafka-console-producer --topic test --broker-list localhost:9092 < test.csv
   ```
1. start kafka consumer
   ```
   kafka-console-consumer --zookeeper localhost:2181 --topic test
   ```


# kafka Java operation

## Make a Kafka stream producer in Java  

1. Write the code
   ```
   Properties props = new Properties();
   props.put("bootstrap.servers", "localhost:9092");
   props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
   props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

   KafkaProducer<String, String> producer = new KafkaProducer<>(props);

   for (int i = 0; i < 100; i++) {
           ProducerRecord<String, String> record = new ProducerRecord<>("test", "value-" + i);
           producer.send(record);
   }
   
   producer.close();
   ```
   The code will generate to a Kafka _test_ topic 100 lines of records.

1. Compile and run
   1. Code should be compiled with _gradle_. First, generate a gradle wrapper
      ```
      gradle wrapper
      ``` 
   1. Compile the code with gradle wrapper
      ```
      ./gradlew build
      ```
   1. Add the following line to specify which class will be run by gradle 
      ```
      mainClassName = 'streaming.KafkaCustomerProducer'
      ``` 
   1. The most straight forward way is to use gradle wrapper
      ```
      ./gradlew run 
      ```
   1. With all dependencies compiled to a fatJar, the package can be submited to _Spark_ engine
      ```
      ./gradlew fatJar
      spark-submit --class streaming.KafkaCustomerProducer streaming.jar
      ``` 
   1. While the code is running, execute a Kafka CLI consumer.
      ```
      kafka-console-consumer --zookeeper localhost:2181 --topic test
      ```
      Records streamed from Kafka Java producer will be received and print out directly to the terminal.
      
   
## Make a Kafka stream consumer in Java  

# Make a Kafka Avro producer in Java  
# Make a Spark Kafka Avro consumer in Java  
# Connect to Schema registry









