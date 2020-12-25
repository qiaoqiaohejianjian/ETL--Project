create table if not exist DATALAKE_DEMO.DEV.dimdate_dlt(
  datetime varchar(30) not null
);

create table if not exist DATALAKE_DEMO.DEV.dimplatform_dlt(
  platform varchar(50) not null
);

create table if not exist DATALAKE_DEMO.DEV.dimsite_dlt(
  site varchar(200) not null
);

create table if not exist DATALAKE_DEMO.DEV.dimvideo_dlt(
  video varchar(200) not null
);

create table if not exist DATALAKE_DEMO.DEV.dimdate(
  date_SKEY autoincrement start 1 increment 1,
  datetime varchar(30) not null
);

create table if not exist DATALAKE_DEMO.DEV.dimplatform(
  platform_SKEY autoincrement start 1 increment 1,
  platform varchar(50) not null
);

create table if not exist DATALAKE_DEMO.DEV.dimsite(
  site_SKEY autoincrement start 1 increment 1,
  site varchar(200) not null
);

create table if not exist DATALAKE_DEMO.DEV.dimvideo(
  video_SKEY autoincrement start 1 increment 1,
  video varchar(200) not null
);

create table if not exist DATALAKE_DEMO.DEV.factvideo(
  date_SKEY,
  platform_SKEY,
  site_SKEY,
  video_SKEY
);


-- populate dlt tables
insert into DATALAKE_DEMO.DEV.dimdate_dlt 
select to_char(datetime,'YYYYMMDDHH24MI') from DATALAKE_DEMO.DEV.CREDIT 
group by to_char(datetime,'YYYYMMDDHH24MI');

insert into DATALAKE_DEMO.DEV.dimplatform_dlt
select plaform from DATALAKE_DEMO.DEV.CREDIT group by platform;

insert into DATALAKE_DEMO.DEV.dimsite_dlt
select site from DATALAKE_DEMO.DEV.CREDIT group by site;

insert into DATALAKE_DEMO.DEV.dimvideo_dlt
select video from DATALAKE_DEMO.DEV.CREDIT group by video;

-- populate dimension tables
insert into DATALAKE_DEMO.DEV.dimdate(datetime)
select f.datetime 
from DATALAKE_DEMO.DEV.dimdate_dlt f 
left join DATALAKE_DEMO.DEV.dimdate t on f.datetime = t.datetime
where t.datetime is NULL;

insert into DATALAKE_DEMO.DEV.dimplatform(platform)
select f.platform 
from DATALAKE_DEMO.DEV.dimplatform_dlt f 
left join DATALAKE_DEMO.DEV.dimplatform t on f.platform = t.platform
where t.platform is NULL;

insert into DATALAKE_DEMO.DEV.dimsite(site)
select f.site 
from DATALAKE_DEMO.DEV.dimsite_dlt f 
left join DATALAKE_DEMO.DEV.dimsite t on f.site = t.site
where t.site is NULL;

insert into DATALAKE_DEMO.DEV.dimvideo(video)
select f.video 
from DATALAKE_DEMO.DEV.dimvideo_dlt f 
left join DATALAKE_DEMO.DEV.dimvideo t on f.video = t.video
where t.video is NULL;

--append fact table
insert into DATALAKE_DEMO.DEV.factvideo
select dd.date_SKEY, dp.platform_SKEY, ds.site_SKEY, dv.video_SKEY
from DATALAKE_DEMO.DEV.CREDIT s
left join DATALAKE_DEMO.DEV.dimdate dd on s.datetime= dd.datetime
left join DATALAKE_DEMO.DEV.platform dp on s.platform= dp.platform
left join DATALAKE_DEMO.DEV.site on ds s.site= ds.site
left join DATALAKE_DEMO.DEV.video on dv s.video= ds.video;
