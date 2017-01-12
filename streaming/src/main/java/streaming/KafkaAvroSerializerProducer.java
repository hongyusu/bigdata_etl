

/*
 *
 * Producer sending Avro message to Kafka stream
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

import org.apache.commons.lang.SerializationException;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.Arrays;
import java.io.FileReader;
import java.io.IOException;

import com.opencsv.CSVReader;

public class KafkaAvroSerializerProducer {

	private static final String PRODtest = "PRODUCE-TEST";

    private static String[] args;

    public static void main(String[] args) throws Exception {
        new KafkaAvroSerializerProducer(args).run();
    }

    public KafkaAvroSerializerProducer(String[] args){
        this.args = args;
    }

    public void run() throws Exception{

        Boolean syncFlag       = false;
        String zookeeperURL    = "http://localhost:2181";
        String bootstrapURL    = "http://10.0.1.2:9092";
        String keySerializer   = "org.apache.kafka.common.serialization.StringSerializer";
        String valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer";
        String inputFilename   = "test.csv";
        String operation       = PRODtest;
        String topic           = "test";

        // parse input arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--sync-flag")) {
                if (args[++i].equals("true")){
                    syncFlag = true;
                } else {
                    syncFlag = false;
                }
            } else if (args[i].equals("--produce-test"))      { operation       = PRODtest;
            } else if (args[i].equals("--input-data-file"))   { inputFilename   = args[++i];
            } else if (args[i].equals("--topic") )            { topic           = args[++i];
            } else if (args[i].equals("--zookeeper-url") )    { zookeeperURL    = args[++i];
            } else if (args[i].equals("--bootstrap-url") )    { bootstrapURL    = args[++i];
            } else if (args[i].equals("--key-serializer") )   { keySerializer   = args[++i];
            } else if (args[i].equals("--value-serializer") ) { valueSerializer = args[++i];
			}
		}

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapURL);
        props.put("key.serializer",    keySerializer);
        props.put("value.serializer",  valueSerializer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        //KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);


        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        if (operation == PRODtest){
            schema = parser.parse(SchemaDefinition.AVRO_SCHEMA_test_1);
            topic  = "test";
        }
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        CSVReader reader = null;
        try{
            reader = new CSVReader( new FileReader(inputFilename) );
            String[] line; 
            int messageCount = 0;
            while ( (line = reader.readNext()) != null ){

                messageCount ++;

                long startTime = System.currentTimeMillis();

                GenericRecord avroRecord = new GenericData.Record(schema);
                for (int i = 0; i < line.length; i++){
                    avroRecord.put(i,line[i]);
                }
                ProducerRecord record = new ProducerRecord<Object, Object>(topic, "key", avroRecord);
                try{
                    producer.send(record, new KafkaAvroSerializerProducerCallback("", messageCount, startTime));
                }catch(SerializationException ex){
                }


                /*
                GenericData.Record avroRecord = new GenericData.Record(schema);
                for (int i = 0; i < line.length; i++){
                    avroRecord.put(i,line[i]);
                }

                byte[] bytes = recordInjection.apply(avroRecord);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);

                if ( syncFlag ){
                    try{
                        producer.send(record).get();
                        System.out.println("MESSAGE(" + messageCount  + ")");
                    } catch (InterruptedException | ExecutionException ex){
                        ex.printStackTrace();
                    }
                } else {
                    producer.send(record, new KafkaAvroSerializerProducerCallback(Arrays.toString(line), messageCount, startTime));
                }
                */

                Thread.sleep(250);
            }
        } catch (IOException ex){
            ex.printStackTrace();
        }

        producer.close();
    }


}



class KafkaAvroSerializerProducerCallback implements Callback {

    private final String message;
    private final int messageCount;
    private final long startTime;

    public KafkaAvroSerializerProducerCallback( String message, int messageCount, long startTime ) {
        this.message      = message;
        this.messageCount = messageCount;
        this.startTime    = startTime;
    }

    public void onCompletion( RecordMetadata metadata, Exception ex ) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if ( metadata != null ) {
            System.out.println(
                    //"MESSAGE---->" + message              + "<----\n" +
                    "MESSAGE("   + messageCount         + "), " + 
                    "PARTITION(" + metadata.partition() + "), " + 
                    "OFFSET("    + metadata.offset()    + "), " +
                    " IN "       + elapsedTime          + " ms");
        } else {
            System.out.println("MESSAGE(" + messageCount + ")"); 
            ex.printStackTrace();
        }
    }

}
