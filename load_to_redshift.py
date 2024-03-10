def load(glue):
    try: 
        response = glue.start_job_run(
                JobName = 'etl1')
        print(response)
        return response['JobRunId']
    except Exception as e:
        print(str(e))
        
    
def glue_job_status(glue, jobrunid):
    status = glue.get_job_run(JobName='etl1', RunId=jobrunid)
    return status















"""
def load(create_engine, sessionmaker):
    conn = create_engine("postgresql+psycopg2://awsadmin&Packlunch8*&redshift-cluster-1.ccm7zvwjanwe.eu-west-1.redshift.amazonaws.com:5439/dev", connect_args={'sslmode': 'prefer'})

    if conn:
        Session = sessionmaker(bind=conn)
        session = Session()

        try:
            query = '''copy hvfhv_fact
from 's3://efenycprocessed/before/'
iam_role 'arn:aws:iam::654654314882:role/redshift_read_s3'
FORMAT AS PARQUET;'''
            session.execute(query)
            print('Insert to core dw complete')
        except Exception as e:
            print(str(e))
        # raise
        finally:
            session.commit()
            session.close()
            print('session closed')
            conn.dispose()
            print('Redshift connection closed')


    else:
        print('Couldnt connect to database for core insert')



























def call_lambda(client, json):
    try:
        response = client.invoke(FunctionName='loadtors', InvocationType='RequestResponse')
        print(json.loads(response['Payload'].read()))
    except Exception as e:
        print(str(e))"""
