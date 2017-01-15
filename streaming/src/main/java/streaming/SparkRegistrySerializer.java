

/*
 *
 * Spark consumer eating Avro message from kafka stream 
 *
 */


package streaming;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.List;

import scala.Tuple2;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import org.apache.spark.storage.StorageLevel;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import java.io.*;
import java.lang.Process;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDecoder;

public class SparkRegistrySerializer {

	private static final String CONSUMEtest     = "CONSUME-test";
	private static final String CONSUMEf2441em  = "CONSUME-f2441em";
	private static final String CONSUMEf2441hm  = "CONSUME-f2441hm";
	private static final String CONSUMEf2441su  = "CONSUME-f2441su";
	private static final String CONSUMEf2441ve  = "CONSUME-f2441ve";
	private static final String CONSUMEf2441vh  = "CONSUME-f2441vh";
	private static final String CONSUMEf2441ya  = "CONSUME-f2441ya";
	private static final String CONSUMEf2441yp  = "CONSUME-f2441yp";
	private static final String CONSUMEt3330bb  = "CONSUME-t3330bb";
	private static final String CONSUMEt3330tr  = "CONSUME-t3330tr";
	private static final String CONSUMEfacpcus  = "CONSUME-facpcus";
	private static final String CONSUMEmulelist = "CONSUME-mulelist";
	private static final String CONSUMEhotlist  = "CONSUME-hotlist";

    private static KafkaProducer<String, byte[]> producerOUT;
    private static KafkaProducer<String, byte[]> producerRBTRAN;
    private static KafkaProducer<String, byte[]> producerAIS;
    private static KafkaProducer<String, byte[]> producerBIS;
    private static KafkaProducer<String, byte[]> producerCIS;
    private static KafkaProducer<String, byte[]> producerNMON;

    private static void setLogLevels() {
        boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
        if (!log4jInitialized) {
            Logger.getLogger(SparkRegistrySerializer.class).info("");
            Logger.getRootLogger().setLevel(Level.WARN);
        }
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
    
    public static void main(String[] args) throws Exception {

        int batchSize       = 3;
        int numThreads      = 1;
        String topics       = "test";
        String zookeeperURL = "localhost:2181";
        String registryURL  = "http://localhost:8081";
        String bootstrapURL = "localhost:9092";
        String groupName    = "mygroup";
        String operation    = CONSUMEtest;

        // parse input arguments
		for (int i = 0; i < args.length; i++) {
			if        (args[i].equals("--batch-size"))        { batchSize    = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--number-threads") )   { numThreads   = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--topics") )           { topics       = args[++i];
            } else if (args[i].equals("--zookeeper-url") )    { zookeeperURL = args[++i];
            } else if (args[i].equals("--registry-url") )     { registryURL  = args[++i];
            } else if (args[i].equals("--bootstrap-url") )    { bootstrapURL = args[++i];
            } else if (args[i].equals("--group") )            { groupName    = args[++i];
			} else if (args[i].equals("--consume-test"))      { operation    = CONSUMEtest;
			}
		}

        SparkConf sparkConf = new SparkConf()
            .setAppName("Spark-Registry-Serializer-Consumer")
            .setMaster("local[2]")
            .registerKryoClasses(
                    new Class<?>[]{
                        Class.forName("org.apache.avro.generic.GenericData"),
                    });

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchSize));

        setLogLevels();

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics.split(",")) {
            topicMap.put(topic, numThreads);
        }

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", zookeeperURL);
        kafkaParams.put("schema.registry.url", registryURL);
        kafkaParams.put("group.id", groupName);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapURL);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producerOUT    = new KafkaProducer<>(props);
        producerRBTRAN = new KafkaProducer<>(props);
        producerAIS    = new KafkaProducer<>(props);
        producerBIS    = new KafkaProducer<>(props);
        producerCIS    = new KafkaProducer<>(props);
        producerNMON   = new KafkaProducer<>(props);

        // TODO: switch to direct kafka stream to consume from brokers directly (KafkaUtils.createDirectStream())
        JavaPairReceiverInputDStream<String, GenericRecord> kafkaMSG = null;

        // define operations
        if (operation == CONSUMEtest){
            // to OUT

            kafkaMSG = KafkaUtils.createStream(
                    jssc,
                    String.class, 
                    GenericRecord.class, 
                    StringDecoder.class, 
                    KafkaAvroDecoder.class, 
                    kafkaParams, 
                    topicMap,
                    StorageLevel.MEMORY_AND_DISK_SER());

            JavaDStream<GenericRecord> avroInMSG = kafkaMSG.map(
                    new Function<Tuple2<String, GenericRecord>,GenericRecord >(){
                        @Override
                        public GenericRecord call(Tuple2<String, GenericRecord> tuple2) throws Exception{
                            return tuple2._2();
                        }
                    });

            JavaDStream<GenericRecord> avroOutMSG = avroInMSG.transform( new TransformTestToTestout() );

            avroOutMSG.foreachRDD(
                    new Function2<JavaRDD<GenericRecord>, Time, Void>(){

                        public Void call(JavaRDD<GenericRecord> rdd, Time time) throws Exception{

                            byte[] bytes   = null;
                            long startTime = 0;
                            ProducerRecord<String, byte[]> data = null;
                            List<GenericRecord> records = null;
                            Broadcast<Map<String,Schema>> schemaList = VariableDefinition.getInstance(new JavaSparkContext(rdd.context()));
                            Injection<GenericRecord, byte[]> outInjection = GenericAvroCodecs.toBinary(schemaList.value().get("OUT"));

                            if (rdd != null){
                                records = rdd.collect();
                                for (GenericRecord record : records){
                                    bytes = outInjection.apply(record);
                                    data = new ProducerRecord<>("GFM.out", bytes);
                                    startTime = System.currentTimeMillis();
                                    producerOUT.send(data, new KafkaProducerCallback(startTime));
                                }
                                System.out.println("----- Message processed: " + rdd.count());
                            }

                            return null;

                        }
                    });

        } else if (operation == CONSUMEf2441em)  {
            // to RBTRAN
        } else if (operation == CONSUMEf2441hm)  {
            // to RBTRAN
        } else if (operation == CONSUMEf2441su)  {
            // to RBTRAN
        } else if (operation == CONSUMEf2441ve)  {
            // to RBTRAN
        } else if (operation == CONSUMEf2441vh)  {
            // to RBTRAN
        } else if (operation == CONSUMEf2441ya)  {
            // to RBTRAN
        } else if (operation == CONSUMEf2441yp)  {
            // to RBTRAN
        } else if (operation == CONSUMEt3330bb)  {
            // to AIS
        } else if (operation == CONSUMEt3330tr)  {
            // to RBTRAN
        } else if (operation == CONSUMEfacpcus)  {
            // to BIS|CIS
        } else if (operation == CONSUMEmulelist) {
            // to mulelist
        } else if (operation == CONSUMEhotlist)  {
            // to hotlist
        }

        jssc.start();
        jssc.awaitTermination();
    }
}



class KafkaProducerCallback implements Callback {

    private final long startTime;

    public KafkaProducerCallback( long startTime ) {
        this.startTime = startTime;
    }

    public void onCompletion( RecordMetadata metadata, Exception ex ) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if ( metadata != null ) {
            System.out.println(
                    "PARTITION(" + metadata.partition() + "), " + 
                    "OFFSET("    + metadata.offset()    + "), " +
                    " IN "       + elapsedTime          + " ms");
        } else {
            ex.printStackTrace();
        }
    }

}

