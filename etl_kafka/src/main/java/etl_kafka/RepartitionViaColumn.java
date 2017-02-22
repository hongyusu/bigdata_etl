/**
 *
 * IN  : <String, GenericRecord>
 * OUT : <GenericRecord.get("key-field"), GenericRecord>
 *
 */

package etl_kafka;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import java.nio.charset.StandardCharsets;

public class RepartitionViaColumn implements KeyValueMapper<String,GenericRecord,KeyValue<String,GenericRecord>>{

    private String fieldname;

	public RepartitionViaColumn(String fieldname) throws Exception{
        this.fieldname = fieldname; 
    }
	   
	@Override
	public KeyValue<String, GenericRecord> apply(String key, GenericRecord value){
        return new KeyValue<>(value.get(fieldname).toString().replaceAll(" ",""),value);
	}
}


