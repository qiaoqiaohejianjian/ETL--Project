#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
# @author: sophieSUN

import boto3
from datetime import datetime
import pandas as pd

import os
import sys
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')  

def lambda_handler(event, context):
    print("lambda function invoked")
    
    for record in event['Records']:
        #bucket name is simple due to restriction of AWS S3
        bucket = record['s3']['bucket']['name']
        #file name is complex since it is customize by users
        key = unquote_plus(record['s3']['object']['key'])
        file_name = key.split("/")[-1]
        print(f"received file: {key}")
        #/tmp is the environment where lambda runs
        local_path = f'/tmp/{file_name}'
        
        s3_client.download_file(bucket, key, local_path)
        print("files in tmp directory: ",os.listdir('/tmp'))
        #process recieved data
        file_list = data_process(local_path)
        print("data transformation is finished")
        print("files now in tmp directory: ",os.listdir('/tmp'))
        #upload all delta tables to AWS S3 folder processed
        for file in file_list:
            s3_client.upload_file(f'/tmp/{file}.csv', bucket, f'processed/{file}.csv')
            print(f"uploaded dlt file to: processed/{file}.csv")


def data_process(file):
    def datetimeObj(x):
        return datetime.strptime(x,'%Y-%m-%dT%H:%M')
    
    def date_format(x):
        xTime = datetime.strptime(x,'%Y-%m-%dT%H:%M')
        xstring = xTime.strftime('%Y%m%d%H%M')
        return datetime.strptime(xstring,'%Y%m%d%H%M')

    def like_platform(x):
        titles = x.split('|')[0]
        if 'Android' in titles: return 'Android'
        elif 'iPhone' in titles: return 'iPhone'
        elif 'iPad' in titles: return 'iPad'
    #    for all other values, return Desktop
        else: return 'Desktop'

    def like_site(x):
        platform_set = {'Android','iPhone','iPad','Web'}
        titles = x.split('|')[0]
        # if titles have something similar to platform, it's a platform, otherwise keeps it as a site
        if any(title in platform_set for title in titles.split()):return None
        return titles

    #read data and specify quote & escape character
    raw = pd.read_csv(file, quotechar='"',escapechar='\\')

    #data auditing: count total #records
    print(f"total recieved records number is {len(raw)}")

    #remove rows without 206 with bool slice
    processed = raw[['206' in el.split(',') for el in raw['events']]]

    #remove rows with VideoTitle.split('|') < 2
    processed = processed[processed['VideoTitle'].apply(lambda x:len(x.split('|'))>1)]

    del raw

    #convert DateTime column to minute grain with datetime object 
    processed.loc[:,'DateTime'] = processed['DateTime'].apply(lambda x: x[:-8])
    dim_time = pd.DataFrame(processed['DateTime'].unique())
 
    dim_time.loc[:,'year'] = dim_time['DateTime'].apply(lambda x: datetimeObj(x).year)
    dim_time.loc[:,'month'] = dim_time['DateTime'].apply(lambda x: datetimeObj(x).month)
    dim_time.loc[:,'day'] = dim_time['DateTime'].apply(lambda x: datetimeObj(x).day)
    dim_time.loc[:,'hour'] = dim_time['DateTime'].apply(lambda x: datetimeObj(x).hour)
    dim_time.loc[:,'minute'] = dim_time['DateTime'].apply(lambda x: datetimeObj(x).minute)
    dim_time.loc[:,'DateTime'] = dim_time.loc[:,'DateTime'].apply(lambda x: date_format(x))

    #convert video titles to title
    processed.loc[:,'title'] = processed['VideoTitle'].apply(lambda x: x.split('|')[-1])
    dim_title = pd.DataFrame(processed['title'].unique())

    #convert video titles to platform
    processed.loc[:,'platform'] = processed['VideoTitle'].apply(lambda x: like_platform(x))
    dim_platform = pd.DataFrame(processed['platform'].unique())

    #convert video titles to site
    processed.loc[:,'site'] = processed['VideoTitle'].apply(lambda x: like_site(x))
    dim_site = pd.DataFrame(processed['site'].unique())

    processed.drop(columns = ['VideoTitle'],inplace=True)
    processed.drop(columns = ['events'],inplace=True)

    fact_dlt = processed
    del processed

    #data auditing: find max len of all field in fact
    for col in fact_dlt.columns:
        print(f"{col} max len is {max([len(i) for i in fact_dlt[col] if i])}")

    # return dim_time,dim_site,dim_platform,dim_title,processed
    #output processed data to csv file
    dim_time.to_csv('/tmp/dim_time.csv',line_terminator='\n',escapechar='\\',index=False)
    dim_site.to_csv('/tmp/dim_site.csv',line_terminator='\n',escapechar='\\',index=False)
    dim_title.to_csv('/tmp/dim_title.csv',line_terminator='\n',escapechar='\\',index=False)
    dim_platform.to_csv('/tmp/dim_platform.csv',line_terminator='\n',escapechar='\\',index=False)
    fact_dlt.to_csv('/tmp/fact.csv',line_terminator='\n',escapechar='\\',index=False)
    return ["dim_time","dim_site","dim_title","dim_platform","fact"]
