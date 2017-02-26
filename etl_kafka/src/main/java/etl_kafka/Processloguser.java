

/*
 *
 * parse from byte to avro
 * input  : kstream<byte[]>
 *        : schema
 * output : kstream<GenericRecord>
 *
 */

package etl_kafka;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import java.nio.charset.StandardCharsets;

public class Processloguser implements ValueMapper<GenericRecord,GenericRecord>{


	public Processloguser() throws Exception{
    }
	   
	@Override
	public GenericRecord apply(GenericRecord avroMSG){

        try{
            avroMSG.put("loguser_PHFGBZRE_VQ",avroMSG.get("loguser_PHFGBZRE_VQ").toString().substring(2));
        }catch(Exception ex){
        }

        return avroMSG;
	}
}



