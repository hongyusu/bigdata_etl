

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

public class ProcessT3330tr implements ValueMapper<GenericRecord,GenericRecord>{


	public ProcessT3330tr() throws Exception{
    }
	   
	@Override
	public GenericRecord apply(GenericRecord avroMSG){

        try{
        }catch(Exception ex){
        }

        return avroMSG;
	}
}



