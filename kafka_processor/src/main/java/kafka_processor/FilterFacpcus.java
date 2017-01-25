

/*
 * 
 *
 */


package kafka_processor;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import java.nio.charset.StandardCharsets;

public class FilterFacpcus implements Predicate<String,GenericRecord>{

	@Override
	public boolean test(String key, GenericRecord avroMSG){
        //return avroMSG.get("facpcus_CUSTOMER_ID").equals("someid");
        return true;
	}
}
