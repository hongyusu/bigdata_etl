/**
 * 
 *
 */


package etl_kafka;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import java.nio.charset.StandardCharsets;

public class FilterloguserForPC implements Predicate<String,GenericRecord>{

   private String keyword = null; 
   public FilterloguserForPC(String keyword) throws Exception{
       this.keyword = keyword;
   }

   @Override
    public boolean test(String key, GenericRecord avroMSG){
        if(avroMSG.get("loguser_PHFGBZRE_VQ").toString().equals(keyword)){
            return true;
        } 
        return false;
    }

}


