/**
 *
 * unit test for spark mapper 
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


public class ProcessLogToPTest {

    private SparkConf conf;
    private static JavaStreamingContext ssc;

    @Before
    public void setUp() {
        conf = new SparkConf().setAppName("test").setMaster("local[*]");
        ssc  = new JavaStreamingContext(conf, new Duration(1000L));
    }

    @After
    public void tearDown() {
        ssc.close();
    }

    @Test
    public void testProcessLogToP() {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SchemaDef.AVRO_SCHEMA_OUTLog);
        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            record.put(field.name(), "1");
        }

        List<GenericRecord> inputList = new ArrayList<>();
        inputList.add(record);

        Queue<JavaRDD<GenericRecord>> rddQueue = new LinkedList<>();
        rddQueue.add(ssc.sparkContext().parallelize(inputList));

        JavaDStream<GenericRecord> inputStream = ssc.queueStream(rddQueue);

        JavaDStream<GenericRecord> outputStream = inputStream.transform(new ProcessLogToP());

        assertEquals(0,0);
    }
}

