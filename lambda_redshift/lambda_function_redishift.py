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
    print('yes!!!!!!')

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
    
    queries = ["CREATE TABLE IF NOT EXISTS time_dlt(\
	'DateTime' DATETIME;",
    
    "CREATE TABLE IF NOT EXISTS title_dlt(\
	title varchar(200));",
    
    "CREATE TABLE IF NOT EXISTS site_dlt(\
	site varchar(10));",
    
    "CREATE TABLE IF NOT EXISTS platform_dlt(\
	platform varchar(20));", 
    #2017-01-11T00:00   strptime(x,'%Y-%m-%dT%H:%M')
    "CREATE TABLE IF NOT EXISTS DIMDATE(\
	DATETIME_SKEY NUMBER( 38,0) not null identity(0,1),\
	'DATETIME' DATETIME,\
	YEAR smallint,\
	MONTH smallint,\
	DAY smallint,\
	HOUR smallint,\
	MINU smallint);",
    
    "CREATE TABLE IF NOT EXISTS DIMTITLE(\
	TITLE_SKEY NUMBER( 38,0) not null identity(0,1),\
	TITLE varchar(200));",
    
    "CREATE TABLE IF NOT EXISTS DIMSITE(\
	SITE_SKEY NUMBER( 38,0) not null identity(0,1),\
	SITE varchar(10));",
    
    "CREATE TABLE IF NOT EXISTS DIMPLATFORM(\
	PLATFORM_SKEY NUMBER( 38,0) not null identity(0,1),\
	PLATFORM varchar(20));",

    "CREATE TABLE IF NOT EXISTS staging(\
	'DATETIME' DATETIME,\
	TITLE varchar(200),\
	PLATFORM varchar(20),\
	SITE varchar(10));",
    
    "CREATE TABLE IF NOT EXISTS FACTVIDEOSTART(\
	factid NUMBER( 38,0) not null identity(0,1),\
	DATETIME_SKEY NUMBER( 38,0),\
	PLATFORM_SKEY NUMBER( 38,0),\
	SITE_SKEY NUMBER( 38,0),\
	TITLE_SKEY NUMBER( 38,0),\
    DB_INSERT_TIMESTAMP TIMESTAMP (6) not null DEFAULT NOW();",

    "COPY time_dlt ('DateTime', 'year', 'month', 'day', 'hour', 'minute')\
    FROM 's3://project4de/processed/dim_time.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;",
    
    "COPY site_dlt ('site')\
    FROM 's3://project4de/processed/dim_site.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;",
    
    "COPY title_dlt ('title')\
    FROM 's3://project4de/processed/dim_title.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;",
    
    "COPY platform_dlt ('platform')\
    FROM 's3://project4de/processed/dim_platform.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;",
    
    "COPY staging ('DATETIME', 'TITLE', 'PLATFORM', 'SITE')\
    FROM 's3://project4de/processed/fact_dlt.csv'\
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole'\
    CSV\
    IGNOREHEADER 1;",

    "insert into DIMDATE ('DATETIME','YEAR','MONTH','DAY','HOUR','MINU')\
    select t.DateTime, YEAR(t.DateTime) as 'YEAR', MONTH(t.DateTime) as 'MONTH',\
    DAY(t.DateTime) as 'DAY', t.DateTime.strftime(%H) as 'HOUR', \
    t.DateTime.strftime(%M) as 'MINU'\
    from time_dlt t left join DIMDATE d on t.DateTime = d.DATETIME\
    where d.DATETIME is null;",
    
    "insert into DIMSITE(SITE)\
    select t.SITE from site_dlt t left join DIMSITE d on t.SITE = d.SITE\
    where d.SITE is null;",

    "insert into DIMTITLE(TITLE)\
    select t.TITLE from title_dlt t left join DIMTITLE d on t.TITLE = d.TITLE\
    where d.TITLE is null;",

    "insert into DIMDIMPLATFORM(DIMPLATFORM)\
    select t.DIMPLATFORM from platform_dlt t left join DIMDIMPLATFORM d\
    on t.DIMPLATFORM = d.DIMPLATFORM\
    where d.DIMPLATFORM is null;",

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
    on e.PLATFORM = d.PLATFORM;",
    
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
