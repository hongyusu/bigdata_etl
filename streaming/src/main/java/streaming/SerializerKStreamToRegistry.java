

/*
 *
 * send Avro to KStream and register server 
 * input  : kstream<String,GenericRecord>
 * output : void 
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
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.commons.lang.SerializationException;

public class SerializerKStreamToRegistry implements ForeachAction<String,GenericRecord>{
    
    private static Properties props;
    private static String topicOut;
    private static KafkaProducer<String, GenericRecord> producer;
	
	public SerializerKStreamToRegistry(Properties props, String topicOut){
        producer = new KafkaProducer<>(props);
        this.topicOut = topicOut;
    }
	
	@Override
	public void apply(String key, GenericRecord avroMSG){
        ProducerRecord record = new ProducerRecord<Object, Object>(topicOut, "key", avroMSG);
        try{
            producer.send(record, new KProducerCallback(System.currentTimeMillis()));
        }catch(SerializationException ex){
            ex.printStackTrace();
        }
	}
	
}

class KProducerCallback implements Callback {

    private final long startTime;

    public KProducerCallback( long startTime ) {
        this.startTime    = startTime;
    }

    public void onCompletion( RecordMetadata metadata, Exception ex ) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if ( metadata != null ) {
            System.out.println(
                    "PARTITION(" + metadata.partition() + "), " + 
                    "OFFSET("    + metadata.offset()    + "), " +
                    " IN "       + elapsedTime          + " ms");
        } else {
            ex.printStackTrace();
        }
    }

}


