use role sysadmin;
drop database DATALAKE_DEMO;
create database DATALAKE_DEMO;
create schema DEV;
create or replace table DATALAKE_DEMO.DEV.SRC_CREDIT
(
  customer_id Text,
  datetime Text,
  attd Text,
  credit_score Text,
  state_id Text,
  type Text
);

----------------------------------------------------DEV-------------------------------------------------
-- SNOW PIPE for SRC_CREDIT
-- Create file format
create or replace file format DATALAKE_DEMO.DEV.FORMAT_SRC_CREDIT
  type = 'csv'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY ='"'
  ;

-- 1. Create an external s3 stage:
create or replace stage DATALAKE_DEMO.DEV.STAGE_SRC_CREDIT
url='s3://your bucket name/'
credentials=(AWS_KEY_ID='XXXXX' AWS_SECRET_KEY='XXXX')
ENCRYPTION=(TYPE = 'AWS_SSE_S3')
;
-- Note: Stage location should be pointed to a folder location and not to a file.

-- 2. Verify the stage is created using:
show stages;

-- 3. Create a pipe using auto_ingest=true:
create or replace pipe DATALAKE_DEMO.DEV.PIPE_SRC_CREDIT auto_ingest = true 
as copy into DATALAKE_DEMO.DEV.SRC_CREDIT
from @DATALAKE_DEMO.DEV.STAGE_SRC_CREDIT
file_format = (format_name = DATALAKE_DEMO.DEV.FORMAT_SRC_CREDIT)
on_error = 'skip_file';

-- 4.Verify the pipe is created using:                                                                                                                                                        
show pipes;

-- 5. Run SHOW STAGES
-- to check for the NOTIFICATION_CHANNEL

-- 6. Setup SQS notification ( https://www.snowflake.net/your-first-steps-with-snowpipe/ )

-- 7. Upload the file CSV file to the static folder in S3

-- 8. Run the following to check if the uploaded file is in your stage location:
ls @DATALAKE_DEMO.DEV.STAGE_SRC_CREDIT; 

-- 9. Wait for 10-15seconds and check the result: 
select * from DATALAKE_DEMO.DEV.SRC_CREDIT;

-- create view to parse source data
CREATE OR REPLACE VIEW DATALAKE_DEMO.DEV.VW_SRC_CREDIT AS
(
  select distinct
  CUSTOMER_ID,
  CONCAT((split_part(DATETIME, ' ', 0)||' '||split_part(DATETIME, ' ', 2)))::datetime AS DATETIME,
  ATTD::FLOAT8 AS ATTD,
  CREDIT_SCORE,
  STATE_ID,
  TYPE
  from DATALAKE_DEMO.DEV.SRC_CREDIT
);

-- define staging table
REATE OR REPLACE TABLE DATALAKE_DEMO.DEV.CREDIT
(
  customer_id Text,
  datetime datetime,
  attd Float8,
  credit_score Text,
  state_id Text,
  type Text
);

-- stream on source table
CREATE OR REPLACE STREAM DATALAKE_DEMO.DEV.STREAM_SRC_CREDIT ON TABLE DATALAKE_DEMO.DEV.SRC_CREDIT;

-- grant sysadmin permission to run task
use role accountadmin;
grant execute task on account to role SYSADMIN;
use ROLE SYSADMIN;

-- define task to MERGE_CREDIT
CREATE OR REPLACE task DATALAKE_DEMO.DEV.MERGE_CREDIT 
  warehouse = TEST_WH
  schedule = '60 minute' 
when system$stream_has_data('DATALAKE_DEMO.DEV.STREAM_SRC_CREDIT')
AS
MERGE INTO DATALAKE_DEMO.DEV.CREDIT dest
  USING DATALAKE_DEMO.DEV.VW_SRC_CREDIT src 
  on src.customer_id = dest.customer_id
  and src.datetime = dest.datetime
  and src.attd = dest.attd
  and src.credit_score = dest.credit_score
  and src.state_id = dest.state_id
  and src.type = dest.type
  WHEN not matched 
    then insert (  
      customer_id, datetime, attd, credit_score, state_id, type)
    values (  
      src.customer_id, src.datetime, src.attd, src.credit_score, src.state_id, src.type);
ALTER TASK DATALAKE_DEMO.DEV.MERGE_CREDIT RESUME;
