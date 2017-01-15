

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

public class ParserByteToAvro implements ValueMapper<byte[],GenericRecord>{

    private static Schema schema;
	private static GenericRecord avroOutMSG;

	public ParserByteToAvro(Schema schema){
        this.schema = schema; 
    }
	   
	@Override
	public GenericRecord apply(byte[] byteMSG){

        avroOutMSG = new GenericData.Record(schema);

        avroOutMSG.put("test_1_field_1","processed__");
        avroOutMSG.put("test_1_field_2","processed__");
        avroOutMSG.put("test_1_field_3","processed__");
        avroOutMSG.put("test_1_field_4","processed__");
        avroOutMSG.put("test_1_field_5","processed__");
        avroOutMSG.put("test_1_field_6","processed__");
        avroOutMSG.put("test_1_field_7","processed__");
        avroOutMSG.put("test_1_field_8","processed__");
        avroOutMSG.put("test_1_field_9","processed__");
        avroOutMSG.put("test_1_field_0","processed__");

        return avroOutMSG;
	}
	
}



