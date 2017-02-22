/**
 *
 *
 *
 */

package etl_kafka;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;

public class Processlogaction implements ValueMapper<GenericRecord, GenericRecord> {

    @Override
    public GenericRecord apply(GenericRecord input) {

        GenericRecord output = new GenericData.Record((GenericData.Record) input, false);

        output.put("logaction_ASIAKASTUNNUS", ((String) input.get("logaction_ASIAKASTUNNUS")).replaceAll(" ", ""));

        return output;
    }
}
