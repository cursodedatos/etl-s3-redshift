import json
import boto3
import time
import csv
import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        #Extract from S3
        
        bucket_name = 'curso-datos-bank'
        file_name = 'bank.csv'
        dir_file = '/tmp/' + file_name
        s3 = boto3.resource('s3')
        s3.meta.client.download_file(bucket_name, file_name, dir_file)

        # Transform Data      
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S" )
        client = boto3.client('redshift-data')
        line_count = 0
        batch_size = 39
        rows_to_insert = []    
        
        with open(dir_file, mode='r') as csv_file:
            data_csv = csv.reader(csv_file,delimiter=';')


            # Transform
            for row in data_csv:
                line_count += 1
                if line_count == 1:
                    pass
                else:      
                    #print(row)
                    SQL_command = "INSERT INTO dev.public.bank VALUES ("+row[0]+",'"+row[1]+"',"+row[5]+",'"+timestamp+"');"
                    rows_to_insert.append(SQL_command)

                if int(line_count/batch_size) == float(line_count/batch_size):
                    print (line_count)
            
                    # Load in Redshift in batch
                    response = client.batch_execute_statement(
                        WorkgroupName ='default-workgroup',
                        Database = 'dev',
                        Sqls = rows_to_insert,
                        SecretArn = 'arn:aws:secretsmanager:us-east-1:799412981296:secret:redshift-Imiurq'
                        )
                    print (response)
                    # Query status 

                    #time.sleep(1)
                    #response_query = client.describe_statement(
                    #    Id = response['Id']
                    #    )
                    #print (response_query )
                    rows_to_insert = []
    
        return {
            'statusCode': 200,
            'body': json.dumps('Succesfully')
        }

    except ValueError as e:
        print ("error main function")
        logger.error(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Server Error')
        }
