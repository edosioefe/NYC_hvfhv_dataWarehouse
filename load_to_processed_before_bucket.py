#pip install s3fs to read data from bucket
#import awswrangler as wr

#pip install aws wrangler
#import awswrangler as wr
# Transfer transformed data to processed S3 bucket
def load_df_to_s3(df, bucket_path, file_name, wr):
# upload to S3 bucket
    try:
        wr.s3.to_parquet(df=df, path= bucket_path + '/' + file_name)
        print("Dataframe to processed bucket successful")
        return True
    except Exception as e:
        print(str(e))
        return False


########
"""
áá
def put_s3(directory, bucket_name, os):
        file_name = os.listdir(directory)[0]
        
        if s3:
            try:
                s3.upload_file(directory + '//' + file_name, bucket_name, file_name)
                print('File has been uploaded')
                return file_name
            except Exception as e:
                print(str(e))
        else:
            print('Couldnt create s3 object')
*/"""
