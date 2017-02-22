/**
 *
 * spark etl process
 *
 */


package etl_spark;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import java.text.*;

import java.util.Date;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;

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

import java.util.concurrent.TimeUnit;


public class ProcessLoguserToP implements Function<JavaRDD<GenericRecord>, JavaRDD<GenericRecord>> {

    private static final long serialVersionUID = 1L;

    @Override
    public JavaRDD<GenericRecord> call(JavaRDD<GenericRecord> rddIn) throws Exception{

        final Broadcast<Map<String,Schema>> schemaList = BCVariables.getInstance(new JavaSparkContext(rddIn.context()));

        JavaRDD<GenericRecord> rddOut = rddIn.map(
                new Function<GenericRecord, GenericRecord>(){

                    @Override
                    public GenericRecord call (GenericRecord input) throws Exception{
                    	
                    	Schema schema = schemaList.value().get("BIS");
                        GenericData.Record output = new GenericData.Record(schema);
                        
                        for (Schema.Field field : schema.getFields()) {
                        	output.put(field.name(),"");
                        }
                        
                        output.put("WORKFLOW_XCD","ABC");
                        return output;             
                    }
                }); 

        return rddOut;
    }
}


