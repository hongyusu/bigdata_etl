/**
 *
 * Test for filter function for loguser
 *
 */

package etl_kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
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

public class ParserByteToAvroTest {

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
    public void testParserByteToAvroOnLoguser() throws Exception {

        KStreamBuilder builder = new KStreamBuilder();

        // SCHEMA
    	Schema.Parser parser  = new Schema.Parser();
        Schema schema_loguser = parser.parse(SchemaDefinition.AVRO_SCHEMA_loguser);

        // MSG
        GenericRecord [] msgOut = new GenericRecord[TestDataLoguser.size];
        for(int k = 0; k < TestDataLoguser.lines.length; k++){
            msgOut[k] = new GenericData.Record(schema_loguser);
            String stringMSG = new String(TestDataLoguser.lines[k].getBytes(), StandardCharsets.UTF_8);
            String[] fields = stringMSG.split(",",-1); 
            try{
                for (int i = 0; i < fields.length; i++){
                    if (fields[i] == null){
                        msgOut[k].put(i,"");
                    }else{
                        msgOut[k].put(i,fields[i]);
                    }
                }
            }catch(Exception ex){
                System.out.println("Error when parsing loguser during tesing loguser processing");
            }
        }

        // STREAM DEFINITION
        KStream<String, byte[]> stream;
        MockProcessorSupplier<String, GenericRecord> processor = new MockProcessorSupplier<>();
        stream = builder.stream(stringSerde, byteArraySerde, topicName);
        stream.mapValues( new ParserByteToAvro(schema_loguser) ).process(processor);

        // DRIVER 
        driver = new KStreamTestDriver(builder);

        // PROCESS DATA
        for (int i = 0; i < TestDataLoguser.size; i++) {
            driver.process(topicName, "key", TestDataLoguser.lines[i].getBytes());
        }

        // TEST SIZE
        assertEquals(TestDataLoguser.size, processor.processed.size());

        // TEST RESULT
        for (int i = 0; i < TestDataLoguser.size; i++) {
            assertEquals("key:"+msgOut[i].toString(), processor.processed.get(i));
        }

    }

}



