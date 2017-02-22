package etl_kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class CustomerJoiner implements ValueJoiner<GenericRecord, GenericRecord, GenericRecord> {

    private Schema schema;
    private String[] rfields;
    private String[] lfields;
    
    public CustomerJoiner(Schema schema) {
        this.schema = schema;
        rfields = null;
        lfields = null;
    }
    
    public CustomerJoiner(Schema schema, String leftFields, String rightFields) {
        this.schema = schema;
        lfields = leftFields.split(",");
        rfields = rightFields.split(",");
    }

    @Override
    public GenericRecord apply(GenericRecord left, GenericRecord right) {
        GenericRecord output = new GenericData.Record(this.schema);

        Object value = null;

        try{
            System.out.print("1-> " + left.get(0)+" "+left.get(1)+" "+left.get(2));
            System.out.print(":");
        }catch(Exception ex){
            System.out.print("1-> ");
        }
        try{
            System.out.println("2-> " + right.get(0)+" "+right.get(1)+" "+right.get(2));
        }catch(Exception ex){
            System.out.println("2-> ");
        }

        for (Schema.Field field : this.schema.getFields()) {
            String name     = field.name();
            String src_name = name;
            if(left != null){
            	value = left.get(src_name); // get returns null if field not found in schema
            }
            if(right != null){
            	value = value == null ? right.get(src_name) : value;
            }
            value = value == null ? "" : value;

            //System.out.println(name+"+"+value);
            output.put(name, value);
        }

        return output;
    }
}
