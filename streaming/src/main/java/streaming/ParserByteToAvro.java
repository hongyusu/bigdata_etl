

/*
 *
 * parse from byte to avro
 * input  : kstream<byte[]>
 *        : schema
 * output : kstream<GenericRecord>
 *
 */

package streaming;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import java.nio.charset.StandardCharsets;

public class ParserByteToAvro implements ValueMapper<byte[],GenericRecord>{

    private static Schema schema;
	private static GenericRecord avroOutMSG;

	public ParserByteToAvro(Schema schema){
        this.schema = schema; 
    }
	   
	@Override
	public GenericRecord apply(byte[] byteMSG){

        avroOutMSG = new GenericData.Record(schema);
        String stringMSG = new String(byteMSG, StandardCharsets.UTF_8);
        String[] fields = stringMSG.split(",");
        for (int i = 0; i < fields.length; i++){
            avroOutMSG.put(i,fields[i]);
        }
        return avroOutMSG;
	}
}



