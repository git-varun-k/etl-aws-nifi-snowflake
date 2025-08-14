create or replace stage scd_demo.scd2.customer_ext_stage
    url = 's3://projekt2-snowflake-nifi/stream_data/'
    credentials = (aws_key_id='aws_key_id' aws_secret_key='aws_secret_key');

create or replace file format scd_demo.scd2.CSV
type = CSV
field_delimiter = ","
skip_header = 1

show stages;
list @CUSTOMER_EXT_STAGE


create or replace pipe customer_s3_pipe
    auto_ingest = true
    as 
    copy into customer_raw
    from @CUSTOMER_EXT_STAGE
    file_format = CSV;

show pipes;
    
select count(*) from customer_raw limit 10;