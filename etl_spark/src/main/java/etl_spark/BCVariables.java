/**
 *
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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

// TODO: read schema definitions directly from registry
public class BCVariables{

    private static volatile Broadcast<Map<String,Schema>> instance = null;

    public static Broadcast<Map<String,Schema>> getInstance(JavaSparkContext jsc){

        Map<String, Schema> ficoSchemaList = new HashMap<>(); 
        int schemaId;
        String registryURL   = "http://localhost:8081";
        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(registryURL,20);

        if(instance == null){
            synchronized (BCVariables.class){
                if (instance == null){

                    Schema SchemaP     = null;
                    Schema SchemaC     = null;

                    try{
                        schemaId = client.getLatestSchemaMetadata("P").getId();
                        SchemaP = client.getByID(schemaId);
                    }catch (Exception ex){
                        SchemaP     = parser.parse(SchemaDef.AVRO_SCHEMA_P   );
                        try{
                            schemaId = client.register("P",SchemaP);
                        }catch(Exception e){
                        }
                    }

                    try{
                        schemaId = client.getLatestSchemaMetadata("C").getId();
                        SchemaC = client.getByID(schemaId);
                    }catch (Exception ex){
                        SchemaC     = parser.parse(SchemaDef.AVRO_SCHEMA_C   );
                        try{
                            schemaId = client.register("C",SchemaC);
                        }catch(Exception e){
                        }
                    }

                    ficoSchemaList.put("P",    SchemaP);
                    ficoSchemaList.put("C",    SchemaC);

                    instance = jsc.broadcast(ficoSchemaList);
                }
            }
        }
        return instance;

    }
}





