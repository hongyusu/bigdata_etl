

/*
 *
 *
 */


package streaming;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

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
import org.apache.spark.storage.StorageLevel;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;


public class TransformTestToTestout implements Function<JavaRDD<GenericRecord>, JavaRDD<GenericRecord>> {

    private static final long serialVersionUID = 1L;

    @Override
    public JavaRDD<GenericRecord> call(JavaRDD<GenericRecord> rddIn) {

        final Broadcast<Map<String,Schema>> schemaList = VariableDefinition.getInstance(new JavaSparkContext(rddIn.context()));

        JavaRDD<GenericRecord> rddOut = rddIn.map(
                new Function<GenericRecord, GenericRecord>(){

                    @Override
                    public GenericRecord call (GenericRecord avroInMSG) throws Exception{
                        GenericData.Record avroOutMSG = new GenericData.Record(schemaList.value().get("OUT"));

                        avroOutMSG.put("out_1_field_1","processed_"+avroInMSG.get("test_1_field_1"));
                        avroOutMSG.put("out_1_field_2","processed_"+avroInMSG.get("test_1_field_2"));
                        avroOutMSG.put("out_1_field_3","processed_"+avroInMSG.get("test_1_field_3"));
                        avroOutMSG.put("out_1_field_4","processed_"+avroInMSG.get("test_1_field_4"));
                        avroOutMSG.put("out_1_field_5","processed_"+avroInMSG.get("test_1_field_5"));
                        avroOutMSG.put("out_1_field_6","processed_"+avroInMSG.get("test_1_field_6"));
                        avroOutMSG.put("out_1_field_7","processed_"+avroInMSG.get("test_1_field_7"));
                        avroOutMSG.put("out_1_field_8","processed_"+avroInMSG.get("test_1_field_8"));
                        avroOutMSG.put("out_1_field_9","processed_"+avroInMSG.get("test_1_field_9"));
                        avroOutMSG.put("out_1_field_0","processed_"+avroInMSG.get("test_1_field_0"));

                        return avroOutMSG;             
                    }
                }); 

        return rddOut;
    }

}


