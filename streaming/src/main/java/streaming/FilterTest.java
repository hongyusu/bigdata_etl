/*
 *
 * 
 * 
 * 
 * 
 *
 */

package streaming;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import java.nio.charset.StandardCharsets;

public class FilterTest implements Predicate<String,GenericRecord>{

	@Override
	public boolean test(String key, GenericRecord avroMSG){
        return avroMSG.get("test_1_field_1").equals("date_0");
	}
}
