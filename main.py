import pandas as pd
import os
import configparser
#pip install s3fs
import awswrangler as wr   
import boto3
import json
import time

from aws_client_object import AwsClient
from ingest import Extract
from transform import transform_data
from load_to_processed_before_bucket import load_df_to_s3
from load_to_redshift import load
from load_to_redshift import glue_job_status
from s3_move_to_after_bucket import to_after_bucket
from s3_delete_from_before_bucket import delete_from_before_bucket

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('config//configuration.ini')

    #config variables
    region_name = config['CONFIG']['region_name']
    access_key_id = config['CONFIG']['access_key_id']
    secret_access_key = config['CONFIG']['secret_access_key']
    staging_bucket_name = config['CONFIG']['staging_bucket_name']
    processed_bucket_name = config['CONFIG']['processed_bucket_name']
    staging_bucket_path = config['CONFIG']['staging_bucket_path']
    processed_before_bucket_path = config['CONFIG']['processed_before_bucket_path']
    processed_after_bucket_path = config['CONFIG']['processed_after_bucket_path']
    staging_directory = config['CONFIG']['staging_directory'] 
    processed_directory = config['CONFIG']['processed_directory']

    # setting up aws client
    aws_object = AwsClient(access_key_id, secret_access_key, region_name, boto3)

    # setting up injestion
    extract = Extract(staging_directory, processed_directory, os, pd)
    
       

# Transform
    # read file from staging bucket
    
    df = extract.extract_file()
    
    # transform data
    
    df2 = transform_data(df, pd)

# Load
    # load data to processed s3 bucket
    if load_df_to_s3(df2, processed_before_bucket_path, extract.return_file_name(), wr):

        # run glue job to load data to redshift; returns glue job id
        runid = load(aws_object.glue_object())
        print('Waiting for redshift load...')
        time.sleep(100)

    # check if glue job has finished running
        if glue_job_status(aws_object.glue_object(), runid)['JobRun']['JobRunState'] == 'SUCCEEDED':
            print('Load to redshift complete.')
            # load to processed after s3 bucket
            try:
                to_after_bucket(aws_object.s3_object(), extract.return_file_name())
                print('Load to processed_after_bucket complete')
                load_to_after = True
            except Exception as e:
                load_to_after = False
                print('Load to processed_after_bucket failed')
                print(str(e))

            # If load completed delete data from processed before s3 bucket   
            if load_to_after:
                delete_from_before_bucket(aws_object.s3_object(), extract.return_file_name())
                print('Delete from processed_before_bucket complete')
            else:
                print('Delete from processed_before_bucket failed')

        # Action to take if data still has not loaded to redshift after 100 seconds
        else:
            print('Load to redshift still running...')
            time.sleep(30)
            if glue_job_status(aws_object.glue_object(), runid)['JobRun']['JobRunState'] == 'SUCCEEDED':
            # load to processed after s3 bucket
                try:
                    to_after_bucket(aws_object.s3_object(), extract.return_file_name())
                    print('Load to processed_after_bucket complete')
                    load_to_after = True
                except Exception as e:
                    load_to_after = False
                    print('Load to processed_after_bucket failed')
                    print(str(e))
                
                if load_to_after:
                    delete_from_before_bucket(aws_object.s3_object(), extract.return_file_name())
                    
                
            else:
                print('Load to redshift not complete, check AWS console.')
            

        
        

            




"""           
                
    df2.to_parquet(processed_directory + '//' +  extract.return_file_name())
    print('Processed dataset has been moved to rocessed directory')
        except Exception as e:
            print(str(e))
        try:
            os.remove(staging_directory + '//' +  extract.return_file_name())
            print('Raw dataset has been deleted from staging directory')
        except Exception as e:
            print(str(e))
    else:
        load = False

    if load:
        load(psycopg2)"""
        