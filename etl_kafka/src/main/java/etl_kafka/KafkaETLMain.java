/**
 *
 * Process Nordea messages from Kafka topics as byte arrays 
 *
 */

package etl_kafka;


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
import org.apache.kafka.streams.kstream.Predicate;
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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

import org.apache.commons.lang.SerializationException;

public class KafkaETLMain {

    private static Serde<GenericRecord> avroSerde;
    private static Serde<String> stringSerde;

    private static String[] args;

    public static void main(String[] args) throws Exception {
        new KafkaETLMain(args).run();
    }

    public KafkaETLMain(String[] args){
        this.args = args;
    }

    public void run() throws Exception{

        String zookeeperURL    = "localhost:2181";
        String bootstrapURL    = "http://10.0.1.2:9092";
        String registryURL     = "http://localhost:8081";
        String keySerializer   = "org.apache.kafka.common.serialization.StringSerializer";
        String valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer";

        // parse input arguments
		for (int i = 0; i < args.length; i++) {

                   if (args[i].equals("--zookeeper-url") )    { zookeeperURL    = args[++i];
            } else if (args[i].equals("--bootstrap-url") )    { bootstrapURL    = args[++i];
            } else if (args[i].equals("--registry-url") )     { registryURL     = args[++i];
            } else if (args[i].equals("--key-serializer") )   { keySerializer   = args[++i];
            } else if (args[i].equals("--value-serializer") ) { valueSerializer = args[++i];
			}
		}
		
		String stamp  = Integer.toString((int) System.currentTimeMillis());

		Properties props = new Properties();
        props.put("schema.registry.url", registryURL  );
        props.put("bootstrap.servers",   bootstrapURL );   
        props.put("application.id",      "Kafka-application" + stamp );
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

        try{
            dataProcessingStream = ProcessLOG(props);
            dataProcessingStream.start();
        }catch(Exception ex){
        }

    }

    private static KafkaStreams ProcessLOG(Properties props) throws Exception{

        System.out.println(">>>>>> Now, processing log");

    	int schemaId;
        Schema.Parser parser  = new Schema.Parser();
    	CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(props.get("schema.registry.url").toString(),20);

        Schema schema_loguser; 
    	try{
            schemaId       = client.getLatestSchemaMetadata("loguser").getId();
            schema_loguser = client.getByID(schemaId);
        }catch (Exception ex){
        	schema_loguser = parser.parse(SchemaDef.AVRO_SCHEMA_loguser);
           try{
        	   schemaId = client.register("loguser",schema_loguser);
           }catch(Exception e){
           }
        }
        
        Schema schema_logaction; 
    	try{
            schemaId       = client.getLatestSchemaMetadata("logaction").getId();
            schema_logaction = client.getByID(schemaId);
        }catch (Exception ex){
        	schema_logaction = parser.parse(SchemaDef.AVRO_SCHEMA_logaction);
           try{
        	   schemaId = client.register("logaction",schema_logaction);
           }catch(Exception e){
           }
        }

        Schema schema_OUTLog; 
    	try{
            schemaId       = client.getLatestSchemaMetadata("OUTLog").getId();
            schema_OUTLog = client.getByID(schemaId);
        }catch (Exception ex){
        	schema_OUTLog = parser.parse(SchemaDef.AVRO_SCHEMA_OUTLog);
           try{
        	   schemaId = client.register("OUTLog",schema_OUTLog);
           }catch(Exception e){
           }
        }

        String topicIn_loguser   = "loguser"; 
        String topicIn_logaction = "logaction";
        String topicOut_OUTLog1  = "OUTLog1";
        String topicOut_OUTLog2  = "OUTLog2";

        // SOURCE
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, byte[]> source_loguser   = builder.stream(topicIn_loguser);
        KStream<String, byte[]> source_logaction = builder.stream(topicIn_logaction);

        // <KTable> logaction
        KTable<String,GenericRecord> KT_logaction_cus = source_logaction
            .mapValues( new GenerateAvroFromByte(schema_logaction) )
            .mapValues( new Processlogaction() )
            .filter( new Filterlogaction() )
            .map( new RepartitionViaColumn("logaction_ASIAKASTUNNUS") )
            .through(stringSerde, avroSerde, "logaction-user")
            .groupByKey(stringSerde, avroSerde)
            .reduce( new Reducer<GenericRecord>(){
                @Override
                public GenericRecord apply(GenericRecord avro1,GenericRecord avro2){
                    return avro1;
                }
            },"KT-logaction-user");

        // <KStream> loguser
        KStream<String, GenericRecord> avroIn_loguser = source_loguser
            .mapValues( new GenerateAvroFromByte(schema_loguser) )
            .mapValues( new Processloguser() )
            .filter( new Filterloguser() )
            .map( new RepartitionViaColumn("loguser_CUSTOMER_ID") )
            .through(stringSerde, avroSerde, "facp-loguser");

        // JOIN : <KStream>loguser + <KTable>logaction
        KStream<String, GenericRecord> loguser_logaction = avroIn_loguser.leftJoin(KT_logaction_cus,
                new CustomerJoiner(schema_OUTLog));

        // BRANCH
        // 0 -> P 
        // 1 -> C 
        KStream<String, GenericRecord>[] loguser_logaction_array = loguser_logaction
            .branch( new FilterloguserForPC("P"), new FilterloguserForPC("C") );

        // STREAM OUT
        loguser_logaction_array[0].to(stringSerde, avroSerde, topicOut_OUTLog1);
        loguser_logaction_array[1].to(stringSerde, avroSerde, topicOut_OUTLog2);

        return new KafkaStreams(builder, props);
    }

}




