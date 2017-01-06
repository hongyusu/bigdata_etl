

/*
 *
 * Spark consumer eating Avro message from kafka stream 
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



public class SparkKafkaConsumer {

	private static final String CONStest = "CONSUME-test";

    private static void setLogLevels() {
        boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
        if (!log4jInitialized) {
            Logger.getLogger(SparkKafkaConsumer.class).info("");
            Logger.getRootLogger().setLevel(Level.WARN);
        }
    }

    public static void main(String[] args) throws Exception {

        int batchSize       = 5;
        int numThreads      = 1;
        String topics       = "test";
        String zookeeperURL = "localhost:2181";
        String groupName    = "mygroup";
        String operation    = CONStest;

        // parse input arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--batch-size"))               { batchSize    = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--number-threads") )   { numThreads   = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--topics") )           { topics       = args[++i];
            } else if (args[i].equals("--zookeeper-url") )    { zookeeperURL = args[++i];
            } else if (args[i].equals("--group") )            { groupName    = args[++i];
			} else if (args[i].equals("--consume-test"))      { operation    = CONStest;
			}
		}

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        setLogLevels();

        SparkConf sparkConf = new SparkConf()
                .setAppName("GFM-Spark-Consumer")
                .setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchSize));

        // topic, thread, zookeeper, group
        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics.split(",")) {
            topicMap.put(topic, numThreads);
        }

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", zookeeperURL);
        kafkaParams.put("group.id", groupName);

        // TODO: switch to direct kafka stream to consume from brokers directly 
        // input byte stream
        JavaPairReceiverInputDStream<String, byte[]> messages = KafkaUtils.createStream(
                jssc,
                String.class, 
                byte[].class, 
                StringDecoder.class, 
                DefaultDecoder.class, 
                kafkaParams, 
                topicMap,
                StorageLevel.MEMORY_AND_DISK_SER());


        // define mapping operations
        if (operation == CONStest){
            JavaDStream<String> lines = messages.map( new MapperTestToTestout() );
            lines.print();
        }


        jssc.start();
        jssc.awaitTermination();
    }
}






