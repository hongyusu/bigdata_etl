/**
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

public class GenerateAvroFromByte implements ValueMapper<byte[],GenericRecord>{

    private Schema schema;
	private GenericRecord avroOutMSG;

	public GenerateAvroFromByte(Schema schema) throws Exception{
        this.schema = schema; 
    }
	   
	@Override
	public GenericRecord apply(byte[] byteMSG){

        avroOutMSG = new GenericData.Record(schema);
        String stringMSG = new String(byteMSG, StandardCharsets.UTF_8);
        String[] fields = stringMSG.split(",",-1);
        try{
            for (int i = 0; i < fields.length; i++){
                if (fields[i] == null){
                    avroOutMSG.put(i,"");
                }else{
                    avroOutMSG.put(i,fields[i]);
                }
            }
        }catch(Exception ex){
            System.out.println("Oops, formatting exception! (escaping chars need to be implemented)");
        }
        return avroOutMSG;
	}
}



