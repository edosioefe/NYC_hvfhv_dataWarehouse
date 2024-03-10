def delete_from_before_bucket(s3, before_bucket_name, filename):
    try:
        s3.delete_object(
    Bucket=before_bucket_name,
    Key=filename,
)
        print('Delete from processed_before_bucket complete')
    except Exception as e:
        print(str(e))