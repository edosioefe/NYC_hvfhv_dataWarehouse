class AwsClient:
    def __init__(self, key_id, secret_key, region_name, boto3):
        
        self.key_id = key_id
        self.secret_key = secret_key
        self.region_name = region_name
        self.boto3 = boto3
            
    def s3_object(self):
        s3 = self.boto3.client('s3', region_name = self.region_name,
                          aws_access_key_id = self.key_id,
                          aws_secret_access_key = self.secret_key)
        return s3
    
    def lambda_object(self):
        lambda_c = self.boto3.client('lambda', region_name = self.region_name,
                          aws_access_key_id = self.key_id,
                          aws_secret_access_key = self.secret_key)
        return lambda_c

    def glue_object(self):
        glue = self.boto3.client('glue', region_name = self.region_name,
                          aws_access_key_id = self.key_id,
                          aws_secret_access_key = self.secret_key)
        return glue

