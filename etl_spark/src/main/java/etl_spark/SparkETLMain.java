/**
 *
 * Spark consumer eating Avro message from kafka stream 
 *
 */


package etl_spark;

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

public class SparkETLMain {

	private static final String ProcessloguserP       ="ProcessloguserP";
	private static final String ProcessloguserC       ="ProcessloguserC";

    private static KafkaProducer<String, byte[]> producerP;
    private static KafkaProducer<String, byte[]> producerC;

    private static void setLogLevels() {
        boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
        if (!log4jInitialized) {
            Logger.getLogger(SparkETLMain.class).info("");
            Logger.getRootLogger().setLevel(Level.WARN);
        }
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }
    
    public static void main(String[] args) throws Exception {

        int batchSize        = 3;
        int numThreads       = 1;
        String topicIn       = null;
        String zookeeperURL  = "localhost:2181";
        String registryURL   = "http://localhost:8081";
        String bootstrapURL  = "localhost:9092";
        String groupName     = "mygroup";
        String operation     = ProcessloguserP;
        String schemaNameOut = null;
        String topicOut      = null;

        // parse input arguments
		for (int i = 0; i < args.length; i++) {
			if        (args[i].equals("--batch-size"))                  { batchSize     = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--number-threads"))              { numThreads    = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--zookeeper-url"))               { zookeeperURL  = args[++i];
            } else if (args[i].equals("--registry-url"))                { registryURL   = args[++i];
            } else if (args[i].equals("--bootstrap-url"))               { bootstrapURL  = args[++i];
            } else if (args[i].equals("--group"))                       { groupName     = args[++i];
            } else if (args[i].equals("--schema-name-out"))             { schemaNameOut = args[++i];
			} else if (args[i].equals("--process-loguser-p"))            { operation     = ProcessloguserP;
			} else if (args[i].equals("--process-loguser-c"))            { operation     = ProcessloguserC;
			}
		}

        SparkConf sparkConf = new SparkConf()
            .setAppName("Spark-Processor")
            .setMaster("local[2]")
            .registerKryoClasses(
                    new Class<?>[]{
                        Class.forName("org.apache.avro.generic.GenericData"),
                    });

        setLogLevels();

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", zookeeperURL);
        kafkaParams.put("schema.registry.url", registryURL);
        kafkaParams.put("group.id", groupName);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapURL);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producerP    = new KafkaProducer<>(props);
        producerC    = new KafkaProducer<>(props);

        // Prepare in/out topics
        if (operation == ProcessloguserP)       {
            topicIn       = "OUT.loguserP";
            topicOut      = "OUT.P";
            schemaNameOut = "P";

        } else if (operation == ProcessloguserC)       {
            topicIn       = "OUT.loguserC";
            topicOut      = "OUT.C";
            schemaNameOut = "C";

        }

        // TODO: DIRECT KAFKA STREAM
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchSize));

        // INPUT TOPIC
        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topicIn.split(",")) {
            topicMap.put(topic, numThreads);
        }
        
        // create key-value stream
        JavaPairReceiverInputDStream<String, GenericRecord> streamMsg = KafkaUtils.createStream(
                jssc,
                String.class, 
                GenericRecord.class, 
                StringDecoder.class, 
                KafkaAvroDecoder.class, 
                kafkaParams, 
                topicMap,
                StorageLevel.MEMORY_AND_DISK_SER());

        // get record stream
        JavaDStream<GenericRecord> avroInMsg = streamMsg.map(
                new Function<Tuple2<String, GenericRecord>,GenericRecord >(){
                    @Override
                    public GenericRecord call(Tuple2<String, GenericRecord> tuple2) throws Exception{
                        return tuple2._2();
                    }
                });


        // processing 
        JavaDStream<GenericRecord> avroOutMsg = null;

        if (operation == ProcessloguserP)       {
            avroOutMsg = avroInMsg.transform( new ProcessLogToP() );
            avroOutMsg.foreachRDD( new ProduceOutputStream(topicOut, schemaNameOut, producerP) );

        } else if (operation == ProcessloguserC)       {
            avroOutMsg = avroInMsg.transform( new ProcessLogToC() );
            avroOutMsg.foreachRDD( new ProduceOutputStream(topicOut, schemaNameOut, producerC) );

        }

        // producer kafka stream
        jssc.start();
        jssc.awaitTermination();
    }

}






