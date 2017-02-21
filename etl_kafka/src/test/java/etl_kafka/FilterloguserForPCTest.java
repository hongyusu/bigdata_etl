/**
 *
 * Test for filter loguser
 *
 */

package etl_kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Array;

public class FilterloguserForPCTest {

    private String topicName = "topic";
    private KStreamTestDriver driver = null;
    private static Serializer serializer     = new KafkaAvroSerializer();
    private static Deserializer deserializer = new KafkaAvroDeserializer();
    private static Serde<String> stringSerde      = Serdes.String();
    private static Serde<GenericRecord> avroSerde = Serdes.serdeFrom(serializer, deserializer);
    private static Serde<byte[]> byteArraySerde   = Serdes.ByteArray();

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testFilter() throws Exception {

        KStreamBuilder builder = new KStreamBuilder();

        // SCHEMA
    	Schema.Parser parser  = new Schema.Parser();
        Schema schema_loguser = parser.parse(SchemaDefinition.AVRO_SCHEMA_loguser);

        // MSG
        GenericRecord [] msgIn = new GenericRecord[TestDataLoguser.size];
        for(int k = 0; k < TestDataLoguser.lines.length; k++){
            msgIn[k] = new GenericData.Record(schema_loguser);
            String stringMSG = new String(TestDataLoguser.lines[k].getBytes(), StandardCharsets.UTF_8);
            String[] fields = stringMSG.split(",",-1); 
            try{
                for (int i = 0; i < fields.length; i++){
                    if (fields[i] == null){
                        msgIn[k].put(i,"");
                    }else{
                        msgIn[k].put(i,fields[i]);
                    }
                }
            }catch(Exception ex){
                System.out.println("Error when parsing loguser during tesing loguser processing");
            }
        }

        // STREAM DEFINITION
        KStream<String, GenericRecord> stream;
        KStream<String, GenericRecord> [] branches;
        MockProcessorSupplier<String, GenericRecord>[] processors;

        stream = builder.stream(stringSerde, avroSerde, topicName);
        branches = stream.branch( new FilterloguserForPC("P"), new FilterloguserForPC("C") );

        assertEquals(2, branches.length);

        processors = (MockProcessorSupplier<String, GenericRecord>[]) Array.newInstance(MockProcessorSupplier.class, branches.length);
        for (int i = 0; i < branches.length; i++) {
            processors[i] = new MockProcessorSupplier<>();
            branches[i].process(processors[i]);
        }

        // DRIVER
        driver = new KStreamTestDriver(builder);

        // TEST
        for (int i = 0; i < TestDataLoguser.size; i++) {
            driver.process(topicName, "key", msgIn[i]);
        }

        System.out.println("----------------");
        System.out.println(processors[0].processed.size());
        System.out.println(processors[1].processed.size());
        assertEquals(23, processors[0].processed.size());
        assertEquals(5, processors[1].processed.size());

    }

}



