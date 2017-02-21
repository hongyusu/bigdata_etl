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

public class RepartitionByField implements KeyValueMapper<String,GenericRecord,KeyValue<String,GenericRecord>>{

    private String[] fieldNames;

	public RepartitionByField(String keyFieldnames) throws Exception{
        this.fieldNames = keyFieldnames.split(","); 
    }
	   
	@Override
	public KeyValue<String, GenericRecord> apply(String key, GenericRecord value){
		String keyValue  = "";
		String seperator = "";
		for(int i = 0; i < fieldNames.length; i++){
			keyValue  = keyValue + seperator + value.get(fieldNames[i]).toString().replaceAll(" ","");
			seperator = " ";
		}
		//System.out.println("keyvalues "+fieldNames[0]+" "+keyv);
        return new KeyValue<>(keyValue,value);
	}
}


