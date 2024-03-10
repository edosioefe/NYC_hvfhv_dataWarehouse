def to_after_bucket(s3, before_bucket_name, after_bucket_name, filename):
    
    s3.copy_object(
    CopySource= before_bucket_name +'/'+ filename,  # /Bucket-name/path/filename
    Bucket= after_bucket_name,                       # Destination bucket
    Key=filename                    # Destination path/filename
)



    