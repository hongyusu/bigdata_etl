

# Installations  

1. _brew_ should be installed first 
   ```bash
   /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
   ```

1. Install Zookeeper and Kafka. Zookeeper is part of this Kafka distribution
   ```bash
   brew install kafka
   ```

1. Install gradle to enable building the project coded in Java
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

1. Add _twitter bijection_ dependencies
   ```
   compile( 'org.apache.avro:avro:1.8.0' )
   compile( 'com.twitter:bijection-avro_2.10:0.9.2' )
   ```
   and import packages
   ```java
   import com.twitter.bijection.Injection;
   import com.twitter.bijection.avro.GenericAvroCodecs;
   ```

1. Schema is defined as a JSON string
   ```java
   public class SchemaDefinition{
   
       public static final String AVRO_SCHEMA_testout =
               "{"
               + "\"type\":\"record\","
               + "\"name\":\"testout\","
               + "\"fields\":["
               + "  {\"name\":\"testout_date\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_time\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_name\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_address\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_country\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_info_6\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_info_7\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_info_8\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_info_9\",\"type\":\"string\" },"
               + "  {\"name\":\"testout_info_0\",\"type\":\"string\" }"
               + "]}";
   
       public static final String AVRO_SCHEMA_test =
               "{"
               + "\"type\":\"record\","
               + "\"name\":\"test\","
               + "\"fields\":["
               + "  {\"name\":\"date\",\"type\":\"string\" },"
               + "  {\"name\":\"time\",\"type\":\"string\" },"
               + "  {\"name\":\"name\",\"type\":\"string\" },"
               + "  {\"name\":\"address\",\"type\":\"string\" },"
               + "  {\"name\":\"country\",\"type\":\"string\" },"
               + "  {\"name\":\"info_6\",\"type\":\"string\" },"
               + "  {\"name\":\"info_7\",\"type\":\"string\" },"
               + "  {\"name\":\"info_8\",\"type\":\"string\" },"
               + "  {\"name\":\"info_9\",\"type\":\"string\" },"
               + "  {\"name\":\"info_0\",\"type\":\"string\" }"
               + "]}";
   }
   ``` 

1. Define Avro schema parser by twitter bijection API
   ```
   Schema.Parser parser = new Schema.Parser();
   Schema schema = null;
   if (operation == PRODtest){
       schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_test);
       topic  = "test";
   }
   Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
   ```

1. Read in a CSV file and for each line in the file parse all fields according to the schema defineition. After that, build a Avro message, encode the message as a byte string, and put the byte string to kafka topic. 
   ```java
   reader = new CSVReader( new FileReader(inputFilename) );
   String[] line; 
   int messageCount = 0;
   while ( (line = reader.readNext()) != null ){
   
       messageCount ++;
   
       long startTime = System.currentTimeMillis();
       GenericData.Record avroRecord = new GenericData.Record(schema);
       for (int i = 0; i < line.length; i++){
           avroRecord.put(i,line[i]);
       }
   
       byte[] bytes = recordInjection.apply(avroRecord);
       ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);
   
       if ( syncFlag ){
           try{
               producer.send(record).get();
               System.out.println("MESSAGE(" + messageCount  + ")");
           } catch (InterruptedException | ExecutionException ex){
               ex.printStackTrace();
           }
       } else {
           producer.send(record, new KafkaAvroProducerCallback(Arrays.toString(line), messageCount, startTime));
       }
   
       Thread.sleep(250);
   }
   ```

1. Add the main class information into gradle build script
   ```
   mainClassName = 'streaming.KafkaAvroProducer'
   ```

1. Run the Kafka Avro producer. While running the producer, execute a Kafka CLI consumer to eat messages from a Kafka topic _test_
   ```bash
   kafka-console-consumer --zookeeper localhost:2181 --topic test
   ```

   The following message are sent as Kafka Avro message
   ```
   date_1,time_1,name_1,address_1,time_1,info_6_1,info_7_1,info_8_1,info_9_1,info_0_1
   date_2,time_2,name_2,address_2,time_2,info_6_2,info_7_2,info_8_2,info_9_2,info_0_2
   ...
   ``` 

   While the CLI consumer will print out the following message as they are Avro messages in byte stream 
   ```
   date_1
         time_1
               name_1address_1
                              time_1info_6_1info_7_1info_8_1info_9_1info_0_1
   
   date_2
         time_2
               name_2address_2
                              time_2info_6_2info_7_2info_8_2info_9_2info_0_2
   ...
   ```

# Make a Spark Kafka Avro consumer in Java  

The implementation described in the previous section makes sure messages are in Avro format before sending into Kafka topic.
In this section, we introduce a consumer as implemented in Spark Streaming framework. The consumer will eat Avro message produced by Kafka Avro producer and process the Avro message via Spark Streaming map reduce operations. 

It's good to point out here the difference between Spark streaming processing/transformation and Kafka processing/transformation is Spark engine is essentially working on a stream of RDDs which are created in a certain time window/interval.

1. Initialize Spark streaming context in which time window to generate RDDs is specified in `Duration.seconds()`
   ```java
   SparkConf sparkConf = new SparkConf()
           .setAppName("GFM-Spark-Consumer")
           .setMaster("local[*]");
   
   JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchSize));
   ```

1. Initialize Spark Stream connector to Kafka topic  
   ```java
   Map<String, String> kafkaParams = new HashMap<>();
   kafkaParams.put("zookeeper.connect", zookeeperURL);
   kafkaParams.put("group.id", groupName);
   JavaPairReceiverInputDStream<String, byte[]> messages = KafkaUtils.createStream(
           jssc,
           String.class, 
           byte[].class, 
           StringDecoder.class, 
           DefaultDecoder.class, 
           kafkaParams, 
           topicMap,
           StorageLevel.MEMORY_AND_DISK_SER());
   ```

1. Data as in variable _message_ from Spark stream is just a byte array. The decoding and transformation should be implemented on top of this byte array.
1. Process the Spark Stream via Spark map reduce operations
   ```java
   if (operation == CONStest){
       JavaDStream<String> lines = messages.map( new MapperTestToTestout() );
       lines.print();
   }
   ```
   Notice that the actual mapper function is implemented via a customerized function.

1. The implemnetaion of the mapper function is as follows
   ```java
   public class MapperTestToTestout implements Function<Tuple2<String, byte[]>, String> {
   
       private static Injection<GenericRecord, byte[]> testInjection;
       static{
           Schema.Parser parserTest = new Schema.Parser();
           Schema schemaTest = parserTest.parse(SchemaDefinition.AVRO_SCHEMA_test);
           testInjection = GenericAvroCodecs.toBinary(schemaTest);
       }
   
       private static final long serialVersionUID = 1L;
   
       @Override
       public String call(Tuple2<String, byte[]> tuple2) {
   
           // output: definition of Testout in Avro 
           Injection<GenericRecord, byte[]> testoutInjection;
           Schema.Parser parserTestout = new Schema.Parser();
           Schema schemaTestout = parserTestout.parse(SchemaDefinition.AVRO_SCHEMA_testout);
           testoutInjection     = GenericAvroCodecs.toBinary(schemaTestout);
           GenericData.Record avroRecordTestout = new GenericData.Record(schemaTestout);
   
           // input: Avro message 
           GenericRecord avroRecordInput = testInjection.invert(tuple2._2()).get();
   
           avroRecordTestout.put("date",avroRecordInput.get("out_date"));
           avroRecordTestout.put("time",avroRecordInput.get("out_time"));
   
           return avroRecordTestout.toString();
       }
   
   }
   ``` 
   The decoding from byte array to Avro message is through `invert()` from _bijection_ package.
   As the function call (mapper) will be executed on every worker, variables used in this mapper call need to be defined as static variables and initialized via Java static initialization
   ```java
    private static Injection<GenericRecord, byte[]> testInjection;
    static{
        Schema.Parser parserTest = new Schema.Parser();
        Schema schemaTest = parserTest.parse(SchemaDefinition.AVRO_SCHEMA_test);
        testInjection = GenericAvroCodecs.toBinary(schemaTest);
    }
   ``` 

   1. Compile and run the Spark kafka consumer. 





 

# Connect to Schema registry









