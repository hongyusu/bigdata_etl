

/*
 *
 * Sending kafka message encoded in Avro 
 *
 */

package streaming;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.Arrays;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import com.opencsv.CSVReader;
import org.apache.commons.codec.binary.Hex;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.ArrayList;
import org.apache.avro.JsonProperties;

import org.apache.commons.lang.SerializationException;

public class KafkaProcessorMain {

	private static final String PROCESSmava     = "PROCESS-MAVA";
	private static final String PROCESSunireal  = "PROCESS-UNIREAL";
	private static final String PROCESScps      = "PROCESS-CPS";
	private static final String PROCESSfacpcus  = "PROCESS-FACPCUS";
	private static final String PROCESSmulelist = "PROCESS-MULELIST";
	private static final String PROCESShotlist  = "PROCESS-HOTLIST";
	private static final String PROCESStest     = "PROCESS-TEST";

    private static String[] args;

    public static void main(String[] args) throws Exception {
        new KafkaProcessorMain(args).run();
    }

    public KafkaProcessorMain(String[] args){
        this.args = args;
    }

    public void run() throws Exception{

        Boolean syncFlag       = false;
        String zookeeperURL    = "localhost:2181";
        String bootstrapURL    = "localhost:9092";
        String keySerializer   = "org.apache.kafka.common.serialization.StringSerializer";
        String valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer";
        String operation       = PROCESStest;

        // parse input arguments
		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--sync-flag")) {
                System.out.println(args[i]);
                if (args[++i].equals("true")){
                    syncFlag = true;
                } else {
                    syncFlag = false;
                }
			} else if (args[i].equals("--produce-mava"))      { operation       = PROCESSmava;
			} else if (args[i].equals("--produce-unireal"))   { operation       = PROCESSunireal;
			} else if (args[i].equals("--produce-cps"))       { operation       = PROCESScps;
			} else if (args[i].equals("--produce-facpcus"))   { operation       = PROCESSfacpcus;
			} else if (args[i].equals("--produce-mulelist"))  { operation       = PROCESSmulelist;
			} else if (args[i].equals("--produce-hotlist"))   { operation       = PROCESShotlist;
			} else if (args[i].equals("--produce-test   "))   { operation       = PROCESStest;
            } else if (args[i].equals("--zookeeper-url") )    { zookeeperURL    = args[++i];
            } else if (args[i].equals("--bootstrap-url") )    { bootstrapURL    = args[++i];
            } else if (args[i].equals("--key-serializer") )   { keySerializer   = args[++i];
            } else if (args[i].equals("--value-serializer") ) { valueSerializer = args[++i];
			}
		}

		Properties props = new Properties();
        props.put("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",  "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("schema.registry.url", "http://10.0.1.2:8081");
        props.put("bootstrap.servers", "10.0.1.2:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-registry-serializer");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*
         * input         : Kstream<String, byte[]>
         * output        : Kstream<String, GenericRecord>
         * GenericRecord : link to schema registry server
         */
        KafkaStreams dataProcessingStream = null;
        String topicOut = null;

        if (operation == PROCESStest) {
            topicOut = "GFM.out"; 
            dataProcessingStream = ProcessTest(props,topicOut);

        } else if (operation == PROCESSmava){
            topicOut = "GFM.mava"; 
            //streams = ProcessMava(props);

        } else if (operation == PROCESSunireal){
            topicOut = "GFM.unireal"; 

        } else if (operation == PROCESScps){
            topicOut = "GFM.cps"; 

        } else if (operation == PROCESSfacpcus){
            topicOut = "GFM.facpcus"; 

        } else if (operation == PROCESShotlist){
            topicOut = "GFM.hotlist"; 

        } else if (operation == PROCESSmulelist){
            topicOut = "GFM.mulelist"; 

        }

        try{
            dataProcessingStream.start();
        }catch(Exception ex){
        }

    }


    // PROCESS TEST
    private static KafkaStreams ProcessTest(Properties props, String topicOut) {

        Schema.Parser parser = new Schema.Parser();
        final Schema schema  = parser.parse(SchemaDefinition.AVRO_SCHEMA_TEST);
        String topicIn       = "test"; 

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, byte[]> source = builder.stream(topicIn);
        KStream<String, GenericRecord> avroInMSG = source.mapValues( new ParserByteToAvro(schema) );
        avroInMSG.foreach( new SerializerKStreamToRegistry(props,topicOut) );

        return new KafkaStreams(builder, props);
    }

}





/*
            Schema schema;
            String topic;
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441em);
            topic  = "GFM.f2441em";
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441hm);
            topic  = "GFM.f2441hm";
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441su);
            topic  = "GFM.f2441su";
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441ve);
            topic  = "GFM.f2441ve";
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441vh);
            topic  = "GFM.f2441vh";
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441ya);
            topic  = "GFM.f2441ya";
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441yp);
            topic  = "GFM.f2441yp";
*/
