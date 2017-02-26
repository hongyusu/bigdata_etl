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

public class ProcessloguserTest {

    private String topicName         = "topic";
    private KStreamTestDriver driver = null;
    private static Serde<String>        stringSerde;
    private static Serde<GenericRecord> avroSerde;

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testProcessloguser() throws Exception {

        Serializer serializer     = new KafkaAvroSerializer();
        Deserializer deserializer = new KafkaAvroDeserializer();
        avroSerde   = Serdes.serdeFrom(serializer, deserializer);
        stringSerde = Serdes.String();

        KStreamBuilder builder = new KStreamBuilder();

        // SCHEMA
    	Schema.Parser parser  = new Schema.Parser();
        Schema schema_loguser = parser.parse(SchemaDef.AVRO_SCHEMA_loguser);

        // MSG
        GenericRecord [] msgIn  = new GenericRecord[TestDataLoguser.size];
        GenericRecord [] msgOut = new GenericRecord[TestDataLoguser.size];
        for(int k = 0; k < TestDataLoguser.lines.length; k++){
            msgIn[k]  = new GenericData.Record(schema_loguser);
            msgOut[k] = new GenericData.Record(schema_loguser);
            String[] fields = TestDataLoguser.lines[0].split(",",-1); 
            try{
                for (int i = 0; i < fields.length; i++){
                    if (fields[i] == null){
                        msgIn[k].put(i,"");
                        msgOut[k].put(i,"");
                    }else{
                        msgIn[k].put(i,fields[i]);
                        msgOut[k].put(i,fields[i]);
                    }
                }
            }catch(Exception ex){
                System.out.println("Error when parsing loguser during tesing loguser processing");
            }
            msgOut[k].put("loguser_PHFGBZRE_VQ",msgOut[k].get("loguser_PHFGBZRE_VQ").toString().substring(2));
        }

        // STREAM DEFINITION
        KStream<String, GenericRecord> stream;
        MockProcessorSupplier<String, GenericRecord> processor = new MockProcessorSupplier<>();
        stream = builder.stream(stringSerde, avroSerde, topicName);
        stream.mapValues(new Processloguser()).process(processor);

        // DRIVER 
        driver = new KStreamTestDriver(builder);

        // PROCESS DATA
        for (int i = 0; i < TestDataLoguser.size; i++) {
            driver.process(topicName, "key", msgIn[i]);
        }

        // TEST SIZE
        assertEquals(TestDataLoguser.size, processor.processed.size());

        // TEST RESULT
        for (int i = 0; i < TestDataLoguser.size; i++) {
            assertEquals("key:"+msgOut[i].toString(), processor.processed.get(i));
        }

    }


}



