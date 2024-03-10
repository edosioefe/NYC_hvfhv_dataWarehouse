#pip install s3fs to read data from bucket

# Gets file from directory and puts it in s3 staging bucket
class Extract:

    def __init__ (self, staging_direc, processed_direc, os, pd):
        self.staging_direc = staging_direc
        self.processed_direc = processed_direc
        self.os = os
        self.pd = pd
    
    def return_file_name(self):
        file_name = self.os.listdir(self.staging_direc)[0]
        return file_name
    
    def extract_file(self):
        if self.os.listdir(self.staging_direc)[0] not in self.os.listdir(self.processed_direc):
            file = self.os.listdir(self.staging_direc)[0]
            df = self.pd.read_parquet(self.staging_direc + '//' + file).head(10000)
            return df
        else:
            print('File has already been processed')


# extract from directory 


# or
# extract > transform > s3 bucket > raw data to folder > delete from staging folder