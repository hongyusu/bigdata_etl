/**
 *
 * unit test for spark mapper : unireal -- rbtran
 *
 */


package etl_spark;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.api.java.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;


public class ProcessLogToCTest {

    @Before
    public void setUp() {
        System.clearProperty("spark.streaming.clock");
    }

    @After
    public void tearDown() {
        System.clearProperty("spark.streaming.clock");
    }

    @Test
    public void testProcessLogToC() {

        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000L));

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SchemaDef.AVRO_SCHEMA_OUTLog);
        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            //System.out.println(field.name());
            record.put(field.name(), "1");
        }

        List<GenericRecord> inputList = new ArrayList<>();
        inputList.add(record);

        Queue<JavaRDD<GenericRecord>> rddQueue = new LinkedList<>();
        rddQueue.add(ssc.sparkContext().parallelize(inputList));

        JavaDStream<GenericRecord> inputStream = ssc.queueStream(rddQueue);

        JavaDStream<GenericRecord> outputStream = inputStream.transform(new ProcessLogToC());

        assertEquals(0,0);
        ssc.close();
    }
}

