use role sysadmin;
drop database DATALAKE_DEMO;
create database DATALAKE_DEMO;
create schema DEV;
-- create source table
create or replace table DATALAKE_DEMO.DEV.SRC_CREDIT
(
  datetime varchar(50),
  video_title varchar(200),
  events varchar(200)
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

-- define staging table which is destination table
CREATE OR REPLACE TABLE DATALAKE_DEMO.DEV.CREDIT
(
  datetime varchar(50),
  platform varchar(200),
  site varchar(200),
  video varchar(200)
);

-- stream on source table
CREATE OR REPLACE STREAM DATALAKE_DEMO.DEV.STREAM_SRC_CREDIT ON TABLE DATALAKE_DEMO.DEV.SRC_CREDIT;

-- grant sysadmin permission to run task
use role accountadmin;
grant execute task on account to role SYSADMIN;
use ROLE SYSADMIN;

-- create task to parse source data
--  only keep video_events contain 206 and discard title.split('|').count=1
CREATE OR REPLACE task DATALAKE_DEMO.DEV.VW_SRC_CREDIT 
  warehouse = TEST_WH
  schedule = '60 minute' 
when system$stream_has_data('DATALAKE_DEMO.DEV.STREAM_SRC_CREDIT')
AS
(
  select to_timestamp(datetime) as datetime,
  case when regexp_like(lower(trim(regexp_substr(videotitle, '[^|]+'))), 'iphone|android|ipad|app') then trim(regexp_substr(videotitle, '[^|]+'))
  when regexp_like(lower(trim(regexp_substr(videotitle, '[^|]+'))), 'news') then 'Desktop'
  else 'unknow'
  end as 'platform',
  case when regexp_like(lower(trim(regexp_substr(videotitle, '[^|]+'))), 'news') then trim(regexp_substr(videotitle, '[^|]+')))
  else 'unkknow' end as 'site',
  trim(regexp_substr(videotitle, '[^|]*$')) as 'video'                                                                                                 
  from DATALAKE_DEMO.DEV.SRC_CREDIT
  where events like '%206%'
  and regexp_count(videotitle,'\|') != 0;
);

-- define task to MERGE_CREDIT
CREATE OR REPLACE task DATALAKE_DEMO.DEV.MERGE_CREDIT 
  after "DATALAKE_DEMO.DEV.VW_SRC_CREDIT"
AS
MERGE INTO DATALAKE_DEMO.DEV.CREDIT dest
  USING DATALAKE_DEMO.DEV.VW_SRC_CREDIT vsrc 
  on vsrc.datetime = dest.datetime
  and vsrc.events = dest.events
  and vsrc.video_title = dest.video_title
  WHEN not matched 
    then insert (  
      datetime, platform, site, video)
    values (  
      vsrc.datetime, vsrc.platform, vsrc.site, vsrc.video );
ALTER TASK DATALAKE_DEMO.DEV.MERGE_CREDIT RESUME;
