import io

def load(final_df, alchemyEngine, table, s3_resource, s3_bucket, s3_filename, load_db_bool=True, load_s3_bool=True):
    responses = []
    if load_db_bool == True:
        db_response = load_db(final_df, alchemyEngine, table)
        responses.append(db_response)
    if load_s3_bool == True:
        s3_response = load_s3(final_df, s3_resource, s3_bucket, s3_filename)
        responses.append(s3_response)
    else:
        responses.append('No Data Loaded')
    return responses


### HELPER FUNCTIONS ###

def load_db(final_df, alchemyEngine, table):
    # Connect to PostgreSQL server
    dbConnection = alchemyEngine.connect()

    # Query PostgreSQL database table and load in DataFrame
    try:
        final_df.to_sql(table, dbConnection, if_exists='fail')
    except Exception as e:  
        print(e)
    else:
        print(f'PostgreSQL Table, "{table}", has been created/ added to successfully.')
    finally:
        dbConnection.close()
        
def load_s3(final_df, s3_resource, s3_bucket, s3_filename):
    # Create a string buffer to store the CSV data
    csv_buffer = io.StringIO()

    # Write the final_df to a CSV format
    final_df.to_csv(csv_buffer, header=True, index=False)

    # Reset the buffer position to 0
    csv_buffer.seek(0)

    # Upload the CSV data to the S3 bucket
    response = s3_resource.Object(s3_bucket, s3_filename).put(Body=csv_buffer.getvalue())

    return response