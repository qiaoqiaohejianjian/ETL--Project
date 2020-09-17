#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
# @author: sophieSUN

import psycopg2
import os
import sys
import boto3
import logging

s3_client = boto3.resource('s3')  

def lambda_handler(event, context):
    print("lambda function invoked")
    print('event:',event)
    rds_host  = os.environ.get('host')
    rds_username = os.environ.get('dbuser')
    rds_user_pwd = os.environ.get('dbpassword')
    rds_db_name = os.environ.get('dbname')
    rds_db_port = os.environ.get('dbport')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    files_set = set()
    for buc in s3_client.buckets.all():   
        for obj in buc.objects.filter(Prefix='processed/'):
            files_set.add(obj.key)

    print("files in processed folder now:", files_set)

    file_list = ["processed/dim_platform.csv",
                "processed/dim_site.csv",
                "processed/dim_time.csv",
                "processed/dim_title.csv",
                "processed/fact.csv"]

    if any(file not in files_set for file in file_list):
        print("not all file exist")
        sys.exit()
    #print('yes!!!!!!')

    try:
        conn = psycopg2.connect(user = rds_username,
                               password = rds_user_pwd,
                               host = rds_host,
                               port = rds_db_port,
                               database = rds_db_name)
    except Exception as e:
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")
    c = conn.cursor()
    
    queries = [
        '''CREATE TABLE IF NOT EXISTS "time_dlt" (
                "Datetime" VARCHAR(20) NOT NULL,
                "year" smallint,
                "month" smallint,
                "day" smallint,
                "hour" smallint,
                "minute" smallint );''',
    
    '''CREATE TABLE IF NOT EXISTS "title_dlt" ("title" VARCHAR(200) NOT NULL);''',
    
    '''CREATE TABLE IF NOT EXISTS "site_dlt" ("site" VARCHAR(10) NOT NULL);''',
    
    '''CREATE TABLE IF NOT EXISTS platform_dlt("platform" VARCHAR(20) NOT NULL);''', 

    '''CREATE TABLE IF NOT EXISTS "DIMDATE" (
	"DATETIME_SKEY" int not null identity(0,1),
	"DATETIME"  VARCHAR(20) NOT NULL);''',
    
    '''CREATE TABLE IF NOT EXISTS "DIMTITLE" (
	"TITLE_SKEY" int not null identity(0,1),
	"TITLE" VARCHAR(200) NOT NULL);''',

    '''CREATE TABLE IF NOT EXISTS "DIMSITE"(
	"SITE_SKEY" int not null identity(0,1),
	"SITE" VARCHAR(10) NOT NULL);''',
    
    '''CREATE TABLE IF NOT EXISTS "DIMPLATFORM" (
	"PLATFORM_SKEY" int not null identity(0,1),
	"PLATFORM" VARCHAR(20) NOT NULL);''',

    '''CREATE TABLE IF NOT EXISTS "staging" (
	"DATETIME" VARCHAR(20) NOT NULL,
	"TITLE" VARCHAR(200) NOT NULL,
	"PLATFORM" VARCHAR(20) NOT NULL,
	"SITE" VARCHAR(10) NOT NULL);''',
    
    '''CREATE TABLE IF NOT EXISTS "FACTVIDEOSTART" (
	"factid" int not null identity(0,1),
	"DATETIME_SKEY" int NOT NULL,
	"PLATFORM_SKEY" int NOT NULL,
	"SITE_SKEY" int NOT NULL,
	"TITLE_SKEY" int NOT NULL);''',

    '''COPY time_dlt ("DateTime", "year", "month", "day", "hour", "minute")\
    FROM 's3://project4de/processed/dim_time.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;''',
    
    '''COPY site_dlt ("site")\
    FROM 's3://project4de/processed/dim_site.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;''',
    
    '''COPY title_dlt ("title")\
    FROM 's3://project4de/processed/dim_title.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;''',
    
    '''COPY platform_dlt ("platform")\
    FROM 's3://project4de/processed/dim_platform.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;''',
    
    '''COPY staging ("DATETIME", "TITLE", "PLATFORM", "SITE")\
    FROM 's3://project4de/processed/fact.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;''',

    '''insert into DIMDATE ("DATETIME")\
    select t.DateTime\
    from time_dlt t left join DIMDATE d on t.DateTime = d.DATETIME\
    where d.DATETIME is null
    order by d.DATETIME_SKEY;''',
    
    "insert into DIMSITE (SITE)\
    select t.SITE from site_dlt t left join DIMSITE d on t.SITE = d.SITE\
    where d.SITE is null\
    order by d.SITE_SKEY;",

    "insert into DIMTITLE (TITLE)\
    select t.TITLE from title_dlt t left join DIMTITLE d on t.TITLE = d.TITLE\
    where d.TITLE is null\
    order by d.TITLE_SKEY;",

    "insert into DIMPLATFORM (PLATFORM)\
    select t.PLATFORM from platform_dlt t left join DIMPLATFORM d\
    on t.PLATFORM = d.PLATFORM\
    where d.PLATFORM is null\
    order by d.PLATFORM_SKEY;",

    "insert into FACTVIDEOSTART (DATETIME_SKEY, TITLE_SKEY, SITE_SKEY, PLATFORM_SKEY)\
    select a.DATETIME_SKEY, b.TITLE_SKEY, c.SITE_SKEY, d.PLATFORM_SKEY\
    from staging e\
    left join DIMDATE a \
    on e.DATETIME = a.DATETIME\
    left join DIMTITLE b\
    on e.TITLE = b.TITLE\
    left join DIMSITE c\
    on e.SITE = e.SITE\
    left join DIMPLATFORM d\
    on e.PLATFORM = d.PLATFORM\
    order by FACTVIDEOSTART.factid;",
    
   " TRUNCATE staging; ",
   " TRUNCATE time_dlt; ",
   " TRUNCATE site_dlt; ",
   " TRUNCATE title_dlt; ",
   " TRUNCATE platform_dlt; "
   ]

    for query in queries:
        print("===============")
        print(query)
        try:
            c.execute(query)
        except Exception as e:
            print(str(e))
            conn.rollback()
            conn.close()
            return { 'statusCode': 500 }
        
    conn.commit()
    conn.close()
    return { 'statusCode': 200, 'body': "" }
