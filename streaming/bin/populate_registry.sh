

#!/usr/bin/sh


schemaTest='{"schema":'\
'  "{\"type\":\"record\",'\
'    \"name\":\"test\",'\
'    \"fields\":['\
'                {\"name\":\"date\",\"type\":\"string\" },'\
'                {\"name\":\"time\",\"type\":\"string\" },'\
'                {\"name\":\"name\",\"type\":\"string\" },'\
'                {\"name\":\"address\",\"type\":\"string\" },'\
'                {\"name\":\"country\",\"type\":\"string\" },'\
'                {\"name\":\"info_6\",\"type\":\"string\" },'\
'                {\"name\":\"info_7\",\"type\":\"string\" },'\
'                {\"name\":\"info_8\",\"type\":\"string\" },'\
'                {\"name\":\"info_9\",\"type\":\"string\" },'\
'                {\"name\":\"info_0\",\"type\":\"string\" }]}"}'

schemaTestout='{"schema":'\
'  "{\"type\":\"record\",'\
'    \"name\":\"testout\",'\
'    \"fields\":['\
'                {\"name\":\"testout_date\",\"type\":\"string\" },'\
'                {\"name\":\"testout_time\",\"type\":\"string\" },'\
'                {\"name\":\"testout_name\",\"type\":\"string\" },'\
'                {\"name\":\"testout_address\",\"type\":\"string\" },'\
'                {\"name\":\"testout_country\",\"type\":\"string\" },'\
'                {\"name\":\"testout_info_6\",\"type\":\"string\" },'\
'                {\"name\":\"testout_info_7\",\"type\":\"string\" },'\
'                {\"name\":\"testout_info_8\",\"type\":\"string\" },'\
'                {\"name\":\"testout_info_9\",\"type\":\"string\" },'\
'                {\"name\":\"testout_info_0\",\"type\":\"string\" }]}"}'



# disable compatibility check
curl -X PUT http://localhost:8081/config \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "NONE"}'
    

# register RBTRAN schema
curl -X POST http://localhost:8081/subjects/schemaTestout/versions \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "${schemaTestout}" 
    

# register f2441em schema 
curl -X POST http://localhost:8081/subjects/schemaTest/versions \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "${schemaTest}" 

# retrive schema: schemaTest with the latest version
curl -X GET -i http://localhost:8081/subjects/schemaTest/versions/latest

# retrive schema: schemaTestout with the latest version
curl -X GET -i http://localhost:8081/subjects/schemaTestout/versions/latest

exit

