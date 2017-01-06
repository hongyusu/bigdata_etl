

/*
 *
 * Avro schema definition 
 *
 */


package streaming;

public class SchemaDefinition{

    public static final String AVRO_SCHEMA_testout =
            "{"
            + "\"type\":\"record\","
            + "\"name\":\"testout\","
            + "\"fields\":["
            + "  {\"name\":\"testout_date\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_time\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_name\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_address\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_time\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_info_6\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_info_7\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_info_8\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_info_9\",\"type\":\"string\" },"
            + "  {\"name\":\"testout_info_0\",\"type\":\"string\" }"
            + "]}";

    public static final String AVRO_SCHEMA_test =
            "{"
            + "\"type\":\"record\","
            + "\"name\":\"test\","
            + "\"fields\":["
            + "  {\"name\":\"date\",\"type\":\"string\" },"
            + "  {\"name\":\"time\",\"type\":\"string\" },"
            + "  {\"name\":\"name\",\"type\":\"string\" },"
            + "  {\"name\":\"address\",\"type\":\"string\" },"
            + "  {\"name\":\"time\",\"type\":\"string\" },"
            + "  {\"name\":\"info_6\",\"type\":\"string\" },"
            + "  {\"name\":\"info_7\",\"type\":\"string\" },"
            + "  {\"name\":\"info_8\",\"type\":\"string\" },"
            + "  {\"name\":\"info_9\",\"type\":\"string\" },"
            + "  {\"name\":\"info_0\",\"type\":\"string\" }"
            + "]}";
}
