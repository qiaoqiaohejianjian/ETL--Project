
# to be continued
import psycopg2
import os
import sys
import boto3
import logging

rds_host  = os.environ.get('host')
rds_username = os.environ.get('user')
rds_user_pwd = os.environ.get('password')
rds_db_name = os.environ.get('database')
rds_db_port = os.environ.get('port')



logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn_string = f"host={rds_host}, user={rds_username}, \
    password={rds_user_pwd}, database={rds_db_name}, port={rds_db_port}"
    conn = psycopg2.connect(conn_string)
except:
    logger.error("ERROR: Could not connect to Postgres instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")


def handler(event, context):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('project4de')
    file_set = set()
    file_list = ["processed/dim_platform.csv",
                "processed/dim_site.csv",
                "processed/dim_time.csv",
                "processed/dim_title.csv",
                "processed/fact.csv"]
    for i in my_bucket.objects.filter(Prefix='processed/'):
        file_set.add(i.key)
        
    if any(file not in file_set for file in file_list):
        print("not all file exist")
        sys.exit()
        
    c = conn.cursor()
    
    queries = ["""
    CREATE TABLE IF NOT EXISTS time(
	timeid int not null identity(0,1),
	DateTime char(16),
	year smallint,
	month smallint,
	day smallint,
	hour smallint,
	minute smallint);
    """,
    
    """
    CREATE TABLE IF NOT EXISTS title(
	titleid int not null identity(0,1),
	title varchar(200));
    """,
    
    """
    CREATE TABLE IF NOT EXISTS site(
	siteid int not null identity(0,1),
	site varchar(10));
    """,
    
    """
    CREATE TABLE IF NOT EXISTS platform(
	platformid int not null identity(0,1),
	platform varchar(20));
    """, 
    
    """
    CREATE TABLE IF NOT EXISTS staging(
	factid int not null identity(0,1),
	DateTime char(16) not null,
	title varchar(200) not null,
	platform varchar(20) not null,
	site varchar(10) not null);
    """,
    
    """
    CREATE TABLE IF NOT EXISTS fact(
	factid int not null identity(0,1),
	timeid int not null,
	titleid int not null,
	siteid int not null,
	platformid int not null);
    """,

    """
    COPY time ("DateTime", "year", "month", "day", "hour", "minute")
    FROM 's3://project4de/processed/dim_time.csv'
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole' 
    CSV
    IGNOREHEADER 1;
    """,
    
    """
    COPY site ("site")
    FROM 's3://project4de/processed/dim_site.csv'
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole' 
    CSV
    IGNOREHEADER 1;
    """,
    
    """
    COPY title ("title")
    FROM 's3://project4de/processed/dim_title.csv'
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole' 
    CSV
    IGNOREHEADER 1;
    """,
    
    """
    COPY platform ("platform")
    FROM 's3://project4de/processed/dim_platform.csv'
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole' 
    CSV
    IGNOREHEADER 1;
    """,
    
    """COPY staging ("DateTime", "title", "platform", "site")
    FROM 's3://project4de/processed/fact.csv'
    credentials 'aws_iam_role=arn:aws:iam::318140223133:role/redshiftRole' 
    CSV
    IGNOREHEADER 1;
    """,
    
    """
    delete from time where timeid > (select min(timeid) from time a where time.datetime = a.datetime);
    delete from site where siteid > (select min(siteid) from site a where site.site = a.site);
    delete from title where titleid > (select min(titleid) from title a where title.title = a.title);
    delete from platform where platformid > (select min(platformid) from platform a where platform.platform = a.platform);
    """,
    
    """
    insert into FACTVIDEOSTART (timeid, titleid, siteid, platformid)
    select a.timeid, b.titleid, c.siteid, d.platformid
    from staging e
    left join DIMTIME a 
    on e.DateTime = a.DateTime
    left join DIMTITLE b 
    on e.title = b.title
    left join DIMSITE c 
    on e.site = e.site
    left join DIMPLATFORM d 
    on e.platform = d.platform;
    """,
    
    """
    TRUNCATE staging;
    """
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
