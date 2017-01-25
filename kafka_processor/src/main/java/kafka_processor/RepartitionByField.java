





/*
 *
 * IN  : <String, GenericRecord>
 * OUT : <GenericRecord.get("key-field"), GenericRecord>
 */

package kafka_processor;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import java.nio.charset.StandardCharsets;

public class RepartitionByField implements KeyValueMapper<String,GenericRecord,KeyValue<String,GenericRecord>>{

    private String keyFieldname;

	public RepartitionByField(String keyFieldname) throws Exception{
        this.keyFieldname = keyFieldname; 
    }
	   
	@Override
	public KeyValue<String, GenericRecord> apply(String key, GenericRecord value){

        return new KeyValue<>(value.get(keyFieldname).toString(),value);
	}
}


