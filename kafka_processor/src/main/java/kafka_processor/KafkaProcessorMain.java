
/*
 *
 * Sending kafka message encoded in Avro 
 *
 */

package kafka_processor;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.JsonProperties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;

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
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import kafka.utils.VerifiableProperties;

import java.util.ArrayList;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.commons.lang.SerializationException;

public class KafkaProcessorMain {

	private static final String PROCESSmava     = "PROCESS-MAVA";
	private static final String PROCESSunireal  = "PROCESS-UNIREAL";
	private static final String PROCESScps      = "PROCESS-CPS";
	private static final String PROCESSfacp     = "PROCESS-FACP";
	private static final String PROCESSmulelist = "PROCESS-MULELIST";
	private static final String PROCESShotlist  = "PROCESS-HOTLIST";
	private static final String PROCESStest     = "PROCESS-TEST";

    private static Serde<GenericRecord> avroSerde;
    private static Serde<String> stringSerde;

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
        String operation       = PROCESSunireal;

        // parse input arguments
		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--sync-flag")) {
                System.out.println(args[i]);
                if (args[++i].equals("true")){
                    syncFlag = true;
                } else {
                    syncFlag = false;
                }
			} else if (args[i].equals("--process-mava"))      { operation       = PROCESSmava;
			} else if (args[i].equals("--process-unireal"))   { operation       = PROCESSunireal;
			} else if (args[i].equals("--process-cps"))       { operation       = PROCESScps;
			} else if (args[i].equals("--process-facp"))      { operation       = PROCESSfacp;
			} else if (args[i].equals("--process-mulelist"))  { operation       = PROCESSmulelist;
			} else if (args[i].equals("--process-hotlist"))   { operation       = PROCESShotlist;
			} else if (args[i].equals("--process-test   "))   { operation       = PROCESStest;
            } else if (args[i].equals("--zookeeper-url") )    { zookeeperURL    = args[++i];
            } else if (args[i].equals("--bootstrap-url") )    { bootstrapURL    = args[++i];
            } else if (args[i].equals("--registry-url") )     { registryURL     = args[++i];
            } else if (args[i].equals("--key-serializer") )   { keySerializer   = args[++i];
            } else if (args[i].equals("--value-serializer") ) { valueSerializer = args[++i];
			}
		}

		Properties props = new Properties();
        props.put("schema.registry.url", registryURL  );
        props.put("bootstrap.servers",   bootstrapURL );
        props.put("application.id",      "GFM-Kafka-Processor" );
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName() );
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName() );

        // customized serialization and deserialization classes
        Serializer serializer     = new KafkaAvroSerializer();
        Deserializer deserializer = new KafkaAvroDeserializer();
        serializer.configure(props,false);
        deserializer.configure(props,false);
        avroSerde   = Serdes.serdeFrom(serializer, deserializer);
        stringSerde = Serdes.String();

        /*
         * input         : Kstream<String, byte[]>
         * output        : Kstream<String, GenericRecord>
         * GenericRecord : automatically links to schema registry server
         */
        KafkaStreams dataProcessingStream = null;

        if (operation == PROCESStest) {
            dataProcessingStream = ProcessTest(props);

        } else if (operation == PROCESSmava){
            dataProcessingStream = ProcessMava(props);

        } else if (operation == PROCESSunireal){
            dataProcessingStream = ProcessUnireal(props);

        } else if (operation == PROCESScps){

        } else if (operation == PROCESSfacp){
            dataProcessingStream = ProcessFACP(props);

        } else if (operation == PROCESShotlist){

        } else if (operation == PROCESSmulelist){

        }

        try{
            dataProcessingStream.start();
        }catch(Exception ex){
        }

    }


    /*
     *
     * PROCESS Unireal 
     * input         : Kstream<String, byte[]> t3330tr
     *                 Kstream<String, byte[]> t3330bb
     *
     * output        : Kstream<String, GenericRecord> GFM.unireal
     *
     * GenericRecord : automatically links to schema registry
     *
     * Logic         : t3330tr + t3330bb -- kafka --> GFM.unireal -- spark --> GFM.rbtran 
     *                 t3330bb -- spark --> GFM.ldgr 
     *
     */
    private static KafkaStreams ProcessUnireal(Properties props) throws Exception{

        System.out.println(">>>>>> Oops, now, processing unireal, given t3330tr + t3330bb + facpcus");

        Schema.Parser parser  = new Schema.Parser();
        final Schema schema_t3330tr    = parser.parse(SchemaDefinition.AVRO_SCHEMA_t3330tr);
        final Schema schema_t3330bb    = parser.parse(SchemaDefinition.AVRO_SCHEMA_t3330bb);
        final Schema schema_GFMunireal = parser.parse(SchemaDefinition.AVRO_SCHEMA_GFMunireal);

        String topicIn_t3330tr     = "t3330tr"; 
        String topicIn_t3330bb     = "t3330bb";
        String topicOut_GFMunireal = "GFM.unireal";

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, byte[]> source_t3330tr = builder.stream(topicIn_t3330tr);
        KStream<String, byte[]> source_t3330bb = builder.stream(topicIn_t3330bb);

        // STREAM IN : t3330tr
        KStream<String, GenericRecord> avroIn_t3330tr = source_t3330tr
            .mapValues( new ParserByteToAvro(schema_t3330tr) )
            .mapValues( new ProcessT3330tr() )
            .filter( new FilterT3330tr() )
            .map( new RepartitionByField("t3330tr_TILINUMERO") )
            .through(stringSerde, avroSerde, "unireal-t3330tr");

        // STREAM IN : t3330bb
        KStream<String, GenericRecord> avroIn_t3330bb = source_t3330bb
            .mapValues( new ParserByteToAvro(schema_t3330bb) )
            .filter( new FilterT3330bb() )
            .map( new RepartitionByField("t3330bb_TILINUMERO") )
            .through(stringSerde, avroSerde, "unireal-t3330bb");

        KTable<String,GenericRecord> KT_t3330bb = avroIn_t3330bb
            .groupByKey(stringSerde, avroSerde)
            .reduce( new Reducer<GenericRecord>(){
                @Override
                public GenericRecord apply(GenericRecord avro1,GenericRecord avro2){
                    System.out.println(avro1.toString());
                    return avro1;
                }
            },"unireal-KT-t3330bb");

        // JOIN
        KStream<String, GenericRecord> t3330tr_t3330bb = avroIn_t3330tr.leftJoin(KT_t3330bb,
                new ValueJoiner<GenericRecord,GenericRecord,GenericRecord>(){
                    @Override
                    public GenericRecord apply(GenericRecord avroT3330tr, GenericRecord avroT3330bb){
                        GenericRecord avroOutMSG = new GenericData.Record(schema_GFMunireal);
                        // TODO: put together two Avro to avroOutMSG
                        try{
                            System.out.print("t3330tr " + avroT3330tr.get(0));
                            System.out.print(":");
                            System.out.print("t3330bb " + avroT3330bb.get(0));
                        }catch(Exception ex){
                        }
                        System.out.println();
                        return avroT3330tr;
                    }
        });

        // STREAM OUT
        t3330tr_t3330bb.to(stringSerde, avroSerde, topicOut_GFMunireal);

        return new KafkaStreams(builder, props);
    }

    /*
     *
     * PROCESS facp 
     * input         : Kstream<String, byte[]> facpcus 
     *                 Kstream<String, byte[]> t3330bb
     *
     * output        : Kstream<String, GenericRecord> GFM.facp
     *
     * GenericRecord : automatically links to schema registry
     *
     */
    private static KafkaStreams ProcessFACP(Properties props) throws Exception{

        System.out.println(">>>>>> Oops, now, processing facp");

        Schema.Parser parser  = new Schema.Parser();
        final Schema schema_facpcus   = parser.parse(SchemaDefinition.AVRO_SCHEMA_facpcus);
        final Schema schema_t3330bb   = parser.parse(SchemaDefinition.AVRO_SCHEMA_t3330bb);
        final Schema schema_GFMfacp   = parser.parse(SchemaDefinition.AVRO_SCHEMA_GFMfacp);

        String topicIn_facpcus  = "facpcus"; 
        String topicIn_t3330bb  = "t3330bb";
        String topicOut_GFMfacp = "GFM.facp";

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, byte[]> source_facpcus = builder.stream(topicIn_facpcus);
        KStream<String, byte[]> source_t3330bb = builder.stream(topicIn_t3330bb);

        // STREAM IN : facpcus
        KStream<String, GenericRecord> avroIn_facpcus = source_facpcus
            .mapValues( new ParserByteToAvro(schema_facpcus) )
            .mapValues( new ProcessFacpcus() )
            .filter( new FilterFacpcus() )
            .map( new RepartitionByField("facpcus_CUSTOMER_ID") )
            .through(stringSerde, avroSerde, "facp-facpcus");

        // STREAM IN : t3330bb
        KStream<String, GenericRecord> avroIn_t3330bb = source_t3330bb
            .mapValues( new ParserByteToAvro(schema_t3330bb) )
            .filter( new FilterT3330bb() )
            .map( new RepartitionByField("t3330bb_ASIAKASTUNNUS") )
            .through(stringSerde, avroSerde, "facp-t3330bb");

        // JOIN
        KStream<String, GenericRecord> facpcus_t3330bb = avroIn_facpcus.leftJoin(avroIn_t3330bb,
                new ValueJoiner<GenericRecord,GenericRecord,GenericRecord>(){
                    @Override
                    public GenericRecord apply(GenericRecord avroFacpcus, GenericRecord avroT3330bb){
                        GenericRecord avroOutMSG = new GenericData.Record(schema_GFMfacp);
                        // TODO: put together two Avro to avroOutMSG
                        try{
                            System.out.print("t3330tr: " + avroFacpcus.get(0));
                            System.out.print(":");
                            System.out.print("t3330bb: " + avroT3330bb.get(2));
                        }catch(Exception ex){
                        }
                        System.out.println();
                        return avroFacpcus;
                    }
        },JoinWindows.of(3000),stringSerde,avroSerde,avroSerde);

        // STREAM OUT
        facpcus_t3330bb.to(stringSerde, avroSerde, topicOut_GFMfacp);

        return new KafkaStreams(builder, props);
    }



    /*
     *
     * PROCESS mava 
     * input         : Kstream<String, byte[]> f2441xx
     *                 Kstream<String, byte[]> t3330bb
     * output        : Kstream<String, GenericRecord> GFM.mava
     * GenericRecord : automatically links to schema registry
     *
     */
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
        //KStream<String, GenericRecord> avroIn = source.mapValues( new ParserByteToAvro(schema) );
        //avroIn.foreach( new SerializerKStreamToRegistry(props,topicOut) );

        return new KafkaStreams(builder, props);
    }


    // PROCESS TEST
    private static KafkaStreams ProcessTest(Properties props) throws Exception{

        Schema.Parser parser = new Schema.Parser();
        final Schema schema  = parser.parse(SchemaDefinition.AVRO_SCHEMA_TEST);
        String topicIn       = "test"; 
        String topicOut      = "GFM.test";

        KStreamBuilder builder = new KStreamBuilder();
        /*
        // GET SOURCE
        KStream<String, byte[]> source1 = builder.stream(topicIn1);

        KStream<String, byte[]> source2 = builder.stream(topicIn2);

        KStream<String, GenericRecord> avroIn1 = source1
            .mapValues( new Preprocessor() )
            .mapValues( new ParserByteToAvro(schema1) )
            .mapValues( new ProcessSource1() )
            .filter( new FilterTest() )
        */

        return new KafkaStreams(builder, props);
    }

}




