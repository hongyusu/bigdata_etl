

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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

        avroRecordTestout.put("testout_date",avroRecordInput.get("date"));
        avroRecordTestout.put("testout_time",avroRecordInput.get("time"));

        return avroRecordTestout.toString();
    }

}



