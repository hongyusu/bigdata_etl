

# Install Kafka

1. Install brew
   ```bash
   /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
   ```

1. Install Kafka and Zookeeper
   ```bash
   brew install kafka
   ```

1. Install gradle
   ```bash
   brew install gradle
   ```

# Kafka CLI operations

1. kafka won't work without zookeep, so start zookeeper
   ```bash
   zkserver start
   ```

1. start kafka server
   ```bash
   kafka-server-start /usr/local/etc/kafka/server.properties
   ```

1. create a topic, _zookeeper URL_, _partition_, and _replication factor_ need to be given as input parameters 
   ```bash
   kafka-topics --zookeeper localhost:2181 --create --topic test --partition 1 --replication-factor 1
   ```

1. start a kafka producer, _broker server url_ need to be specified
   ```bash
   kafka-console-producer --topic test --broker-list localhost:9092
   ```

   or send to stream content in file
   ```bash
   kafka-console-producer --topic test --broker-list localhost:9092 < test.csv
   ```

1. start kafka consumer
   ```bash
   kafka-console-consumer --zookeeper localhost:2181 --topic test
   ```


# kafka Java operation

## Dependency

The following _kafka_ and _spark_ versions are compatible and should work together. In short, kafka version 0.10.1.0 should be paied with spark version 1.6.2. In practice, the following _gradle_ dependencies need to be added to build script.
```
compile( 'org.apache.kafka:kafka-clients:0.10.1.0' )           
compile( 'org.apache.kafka:kafka_2.10:0.10.1.0' )              
compile( 'org.apache.spark:spark-core_2.10:1.6.2')             
compile( 'org.apache.spark:spark-streaming_2.10:1.6.2')        
compile( 'org.apache.spark:spark-streaming-kafka_2.10:1.6.2' ) 
```

## Make a Kafka stream producer in Java  

1. Write the code
   ```java
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
      ```bash
      gradle wrapper
      ``` 

   1. Compile the code with gradle wrapper
      ```bash
      ./gradlew build
      ```

   1. Add the following line to specify which class will be run by gradle 
      ```bash
      mainClassName = 'streaming.KafkaCustomerProducer'
      ``` 

   1. The most straight forward way is to use gradle wrapper
      ```bash
      ./gradlew run 
      ```

   1. To build a fatJar add the following stuffs to `build.gradle`
      ```
      task fatJar(type: Jar){
          zip64 true
          description = "Assembles a Hadoop ready fat jar file" 
          baseName = project.name + '-all' 
          doFirst {
          from {
                  configurations.compile.collect { it.isDirectory() ? it : zipTree(it) }
          }
          }
          manifest {
                  attributes( "Main-Class": "${archivesBaseName}/${mainClassName}")
          }
          exclude 'META-INF/*.RSA','META-INF/*.SF','META-INF/*.DSA'
          with jar 
      }
      ```

      With all dependencies compiled to a fatJar, the package can be submited to _Spark_ engine
      ```bash
      ./gradlew fatJar
      spark-submit --class streaming.KafkaCustomerProducer streaming.jar
      ``` 

   1. While the code is running, execute a Kafka CLI consumer.
      ```bash
      kafka-console-consumer --zookeeper localhost:2181 --topic test
      ```

      Records streamed from Kafka Java producer will be received and print out directly to the terminal.
      
   
## Make a Kafka stream consumer in Java  

1. Write code 
   ```java
   Properties props = new Properties();
   props.put("bootstrap.servers", "localhost:9092");
   props.put("group.id", "mygroup");
   props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
   props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
   
   KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
   consumer.subscribe(Arrays.asList("test"));
   
   boolean running = true;
   while (running) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
        }
   }
   
   consumer.close();
   ```
1. Compile and run 
   1. Add the following line to specify which class will be run by gradle 
      ```
      mainClassName = 'streaming.KafkaCustomerConsumer'
      ``` 

   1. Follow the same principle, the code can be run via _gradle_ wrapper or _spark_.
   1. While the Java Kafka consumer is running, execute a Kafka CLI producer
      ```bash
      kafka-console-producer --topic test --broker-list localhost:9092
      ```  

      or 
      ```bash
      kafka-console-producer --topic test --broker-list localhost:9092 < test.csv
      ```  

      messages populated to Kafka topic _test_ will be consumed and printed out to the terminal.

# Make a Kafka Avro producer in Java  

# Make a Spark Kafka Avro consumer in Java  

# Connect to Schema registry









