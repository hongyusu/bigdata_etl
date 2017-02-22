

/*
 *
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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.storage.StorageLevel;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;


public class ProduceOutputStream implements Function2<JavaRDD<GenericRecord>, Time, Void> {

    private String topicOut;
    private String schemaNameOut; 
    private KafkaProducer<String, byte[]> producer;

    public ProduceOutputStream(String topicOut, String schemaNameOut, KafkaProducer<String, byte[]> producer){
        this.topicOut      = topicOut;
        this.schemaNameOut = schemaNameOut;
        this.producer      = producer;
    }

    public Void call(JavaRDD<GenericRecord> rdd, Time time) throws Exception{

        long startTime = 0;
        byte[] bytes   = null;
        ProducerRecord<String, byte[]> data = null;
        List<GenericRecord> records = null;
        Broadcast<Map<String,Schema>> schemaList = BCVariables.getInstance(new JavaSparkContext(rdd.context()));
        Injection<GenericRecord, byte[]> outInjection = GenericAvroCodecs.toBinary(schemaList.value().get(schemaNameOut));

        if (rdd != null){
            records = rdd.collect();
            for (GenericRecord record : records){
                bytes = outInjection.apply(record);
                data = new ProducerRecord<>(topicOut, bytes);
                startTime = System.currentTimeMillis();
                producer.send(data, new KafkaProducerCallback(startTime));
            }
            System.out.println("----- Message processed: " + rdd.count());
        }

        return null;
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


