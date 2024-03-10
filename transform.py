# Read data from s3 staging bucket 
#pip install s3fs to read data from bucket
#use s3 object


# Transformation
def transform_data(df, pd):
    
    try:
        #change to seperate date and time column
        df['request_date_id'] = df['request_datetime'].dt.date
        df['on_scene_date_id'] = df['on_scene_datetime'].dt.date
        df['pickup_date_id'] = df['pickup_datetime'].dt.date
        df['dropoff_date_id'] = df['dropoff_datetime'].dt.date

        df['request_time_id'] = df['request_datetime'].dt.time
        df['on_scene_time_id'] = df['on_scene_datetime'].dt.time
        df['pickup_time_id'] = df['pickup_datetime'].dt.time
        df['dropoff_time_id'] = df['dropoff_datetime'].dt.time

        # change columns to string
        df['request_date_id'] = df['request_date_id'].astype('str')
        df['on_scene_date_id'] = df['on_scene_date_id'].astype('str')
        df['pickup_date_id'] = df['pickup_date_id'].astype('str')
        df['dropoff_date_id'] = df['dropoff_date_id'].astype('str')

        df['request_time_id'] = df['request_time_id'].astype('str')
        df['on_scene_time_id'] = df['on_scene_time_id'].astype('str')
        df['pickup_time_id'] = df['pickup_time_id'].astype('str')
        df['dropoff_time_id'] = df['dropoff_time_id'].astype('str')

        # Remove special characters 
        df['request_date_id'] = df['request_date_id'].replace('-','', regex=True)
        df['on_scene_date_id'] = df['on_scene_date_id'].replace('-','', regex=True)
        df['pickup_date_id'] = df['pickup_date_id'].replace('-','', regex=True)
        df['dropoff_date_id'] = df['dropoff_date_id'].replace('-','', regex=True)

        df['request_time_id'] = df['request_time_id'].replace(':','', regex=True)
        df['on_scene_time_id'] = df['on_scene_time_id'].replace(':','', regex=True)
        df['pickup_time_id'] = df['pickup_time_id'].replace(':','', regex=True)
        df['dropoff_time_id'] = df['dropoff_time_id'].replace(':','', regex=True)

        # Check if any date and time ids are NAT and change Nat to 99999999 or 999999; 
        def replace_date_nat(date):
            date_id = date
            if date_id == 'NaT':
                return '99999999'
            else:
                return date_id
        def replace_time_nat(time):
            time_id = time
            if time_id == 'NaT':
                return '999999'
            else:
                return time_id
    
        df['request_date_id'] = df['request_date_id'].apply( replace_date_nat)
        df['on_scene_date_id'] = df['on_scene_date_id'].apply( replace_date_nat)
        df['pickup_date_id'] = df['pickup_date_id'].apply( replace_date_nat)
        df['dropoff_date_id'] = df['dropoff_date_id'].apply( replace_date_nat)

        df['request_time_id'] = df['request_time_id'].apply( replace_time_nat)
        df['on_scene_time_id'] = df['on_scene_time_id'].apply( replace_time_nat)
        df['pickup_time_id'] = df['pickup_time_id'].apply( replace_time_nat)
        df['dropoff_time_id'] = df['dropoff_time_id'].apply( replace_time_nat)


        #change columns back to int
        df['request_date_id'] = df['request_date_id'].astype('int')
        df['on_scene_date_id'] = df['on_scene_date_id'].astype('int')
        df['pickup_date_id'] = df['pickup_date_id'].astype('int')
        df['dropoff_date_id'] = df['dropoff_date_id'].astype('int')

        df['request_time_id'] = df['request_time_id'].astype('int')
        df['on_scene_time_id'] = df['on_scene_time_id'].astype('int')
        df['pickup_time_id'] = df['pickup_time_id'].astype('int')
        df['dropoff_time_id'] = df['dropoff_time_id'].astype('int')

        # LOCATION change name 
        df['pu_location_id'] = df['PULocationID']
        df['do_location_id'] = df['DOLocationID']

        #hvfhs_license_num change name 
        df['company_id'] = df['hvfhs_license_num']

        # funtion to create shared ride id numbers
        def shared_ride_func(shared):
                shared_request_flag,shared_match_flag = shared
                if (shared_request_flag == 'Y') & (shared_match_flag == 'Y'):
                    return 1
                elif (shared_request_flag == 'Y') & (shared_match_flag == 'N'):
                    return 2
                elif (shared_request_flag == 'N') & (shared_match_flag == 'Y'):
                    return 3
                elif (shared_request_flag == 'N') & (shared_match_flag == 'N'):
                    return 4
                else:
                    return 999
        df['shared_ride_id'] = df[['shared_request_flag','shared_match_flag']].apply(shared_ride_func,axis=1)
        df['shared_ride_id'] = df['shared_ride_id'].astype('int')

        #extra charges fact measure
        df['extra_charges'] =  df['tolls'] + df['bcf'] + df['sales_tax'] + df['congestion_surcharge'] + df['airport_fee']

        #final changes
        df = df[['request_date_id', 'on_scene_date_id', 'pickup_date_id','dropoff_date_id', 
            'request_time_id', 'on_scene_time_id', 'pickup_time_id','dropoff_time_id', 
            'pu_location_id', 'do_location_id', 'company_id', 'shared_ride_id', 'trip_miles', 'trip_time','base_passenger_fare', 'tips', 'extra_charges']]
        
        print('Transformation successful.')
        return df
        

    except Exception as e:
         print(str(e))