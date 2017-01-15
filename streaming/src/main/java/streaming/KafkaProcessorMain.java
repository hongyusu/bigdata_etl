

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
        String bootstrapURL    = "http://10.0.1.2:9092";
        String registryURL     = "http://localhost:8081";
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
            } else if (args[i].equals("--registry-url") )     { registryURL     = args[++i];
            } else if (args[i].equals("--key-serializer") )   { keySerializer   = args[++i];
            } else if (args[i].equals("--value-serializer") ) { valueSerializer = args[++i];
			}
		}

		Properties props = new Properties();
        props.put("key.serializer",    keySerializer   );
        props.put("value.serializer",  valueSerializer );
        props.put("schema.registry.url", registryURL  );
        props.put("bootstrap.servers",   bootstrapURL );
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

        if (operation == PROCESStest) {
            dataProcessingStream = ProcessTest(props);

        } else if (operation == PROCESSmava){
            dataProcessingStream = ProcessMava(props);

        } else if (operation == PROCESSunireal){
            dataProcessingStream = ProcessUnireal(props);

        } else if (operation == PROCESScps){

        } else if (operation == PROCESSfacpcus){

        } else if (operation == PROCESShotlist){

        } else if (operation == PROCESSmulelist){

        }

        try{
            dataProcessingStream.start();
        }catch(Exception ex){
        }

    }


    // PROCESS TEST
    private static KafkaStreams ProcessTest(Properties props) {

        Schema.Parser parser = new Schema.Parser();
        final Schema schema  = parser.parse(SchemaDefinition.AVRO_SCHEMA_TEST);
        String topicIn       = "test"; 
        String topicOut      = "GFM.test";

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, byte[]> source = builder.stream(topicIn);
        KStream<String, GenericRecord> avroInMSG = source
            //.mapValues( new Preprocessor() )
            .mapValues( new ParserByteToAvro(schema) )
            .filter( new FilterTest() );
        avroInMSG.foreach( new SerializerKStreamToRegistry(props,topicOut) );

        return new KafkaStreams(builder, props);
    }

    // PROCESS Unireal 
    private static KafkaStreams ProcessMava(Properties props) {

        Schema.Parser parser = new Schema.Parser();

        final Schema schemaF2441em  = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441em);
        final Schema schemaF2441hm  = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441hm);
        final Schema schemaF2441su  = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441su);
        final Schema schemaF2441ve  = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441ve);
        final Schema schemaF2441vh  = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441vh);
        final Schema schemaF2441ya  = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441ya);
        final Schema schemaF2441yp  = parser.parse(SchemaDefinition.AVRO_SCHEMA_f2441yp);

        String topicInF2441em = "f2441em";
        String topicInF2441hm = "f2441hm";
        String topicInF2441su = "f2441su";
        String topicInF2441ve = "f2441ve";
        String topicInF2441vh = "f2441vh";
        String topicInF2441ya = "f2441ya";
        String topicInF2441yp = "f2441yp";
        String topicOut       = "GFM.mavaRBTRAN";

        KStreamBuilder builder = new KStreamBuilder();
        //KStream<String, byte[]> source = builder.stream(topicIn);
        //KStream<String, GenericRecord> avroInMSG = source.mapValues( new ParserByteToAvro(schema) );
        //avroInMSG.foreach( new SerializerKStreamToRegistry(props,topicOut) );

        return new KafkaStreams(builder, props);
    }


    // PROCESS Unireal 
    private static KafkaStreams ProcessUnireal(Properties props) {

        Schema.Parser parser = new Schema.Parser();
        final Schema schema  = parser.parse(SchemaDefinition.AVRO_SCHEMA_t3330bb);
        String topicT3330bb       = "t3330bb"; 
        String topicUnirealAIS    = "GFM.unirealAIS";
        String topicUnirealRBTRAN = "GFM.unirealRBTRAN";

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, byte[]> source = builder.stream(topicT3330bb);
        KStream<String, GenericRecord> avroInMSG = source.mapValues( new ParserByteToAvro(schema) );
        avroInMSG.foreach( new SerializerKStreamToRegistry(props,topicUnirealAIS) );

        return new KafkaStreams(builder, props);
    }


}




