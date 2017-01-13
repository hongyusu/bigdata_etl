

/*
 *
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


// TODO: read schema definitions directly from registry
public class VariableDefinition{

    private static volatile Broadcast<Map<String,Schema>> instance = null;

    public static Broadcast<Map<String,Schema>> getInstance(JavaSparkContext jsc){

        Map<String, Schema> ficoSchemaList = new HashMap<>(); 

        if(instance == null){
            synchronized (VariableDefinition.class){
                if (instance == null){
                    Schema.Parser parser = new Schema.Parser();

                    Schema schemaOUT     = parser.parse(SchemaDefinition.AVRO_SCHEMA_OUT);
                    Schema schemaRBTRAN  = parser.parse(SchemaDefinition.AVRO_SCHEMA_RBTRAN);
                    Schema schemaAIS     = parser.parse(SchemaDefinition.AVRO_SCHEMA_AIS   );
                    Schema schemaBIS     = parser.parse(SchemaDefinition.AVRO_SCHEMA_BIS   );
                    Schema schemaCIS     = parser.parse(SchemaDefinition.AVRO_SCHEMA_CIS   );
                    Schema schemaNMON    = parser.parse(SchemaDefinition.AVRO_SCHEMA_NMON  );

                    ficoSchemaList.put("OUT",    schemaOUT);
                    ficoSchemaList.put("RBTRAN", schemaRBTRAN);
                    ficoSchemaList.put("AIS",    schemaAIS);
                    ficoSchemaList.put("BIS",    schemaBIS);
                    ficoSchemaList.put("CIS",    schemaCIS);
                    ficoSchemaList.put("NMON",   schemaNMON);

                    instance = jsc.broadcast(ficoSchemaList);
                }
            }
        }
        return instance;

    }
}





