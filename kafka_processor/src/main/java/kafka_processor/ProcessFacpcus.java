

/*
 *
 * parse from byte to avro
 * input  : kstream<byte[]>
 *        : schema
 * output : kstream<GenericRecord>
 *
 */

package kafka_processor;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import java.nio.charset.StandardCharsets;

public class ProcessFacpcus implements ValueMapper<GenericRecord,GenericRecord>{


	public ProcessFacpcus() throws Exception{
    }
	   
	@Override
	public GenericRecord apply(GenericRecord avroMSG){

        try{
            avroMSG.put("facpcus_CUSTOMER_ID",avroMSG.get("facpcus_CUSTOMER_ID").toString().substring(2));
        }catch(Exception ex){
        }

        return avroMSG;
	}
}



