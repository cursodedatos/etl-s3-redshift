import json
import boto3
import csv
import logging
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME = 'bucket-name'
FILE_NAME = 'bank.csv'
DIR_FILE = '/tmp/' + FILE_NAME
BATCH_SIZE = 39
REDSHIFT_SECRET_ARN =  "arn:aws:secretsmanager:us-east-1:XXXXXXXXX:secret:secreto-redshift-XXXXXX"
REDSHIFT_WORKGROUP = 'default-workgroup'
REDSHIFT_DATABASE = 'dev'
REDSHIFT_TABLE = 'dev.public.bank'

def lambda_handler(event, context):
    try:
        extract_csv_from_s3(BUCKET_NAME, FILE_NAME, DIR_FILE)
        client = boto3.client('redshift-data')
        rows_to_insert = parse_csv_to_sql(DIR_FILE)

        insert_batches_to_redshift(rows_to_insert, client)

        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed and inserted data.')
        }

    except Exception as e:
        logger.error("Unhandled error:\n%s", traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps('Server Error: ' + str(e))
        }

def extract_csv_from_s3(bucket, file_name, dest_path):
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket, file_name, dest_path)
    logger.info("Downloaded file from S3: %s", file_name)

def parse_csv_to_sql(file_path):
    rows_to_insert = []

    with open(file_path, mode='r') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        header_skipped = False

        for row in reader:
            if not header_skipped:
                header_skipped = True
                continue

            if len(row) != 17:
                logger.warning("Fila ignorada (esperadas 17 columnas, recibidas %d): %s", len(row), row)
                continue

            # Escapar comillas simples
            safe_row = [col.replace("'", "''") for col in row]

            SQL_command = (
                f"INSERT INTO {REDSHIFT_TABLE} VALUES ("
                f"{safe_row[0]}, '{safe_row[1]}', '{safe_row[2]}', '{safe_row[3]}', '{safe_row[4]}', "
                f"{safe_row[5]}, '{safe_row[6]}', '{safe_row[7]}', '{safe_row[8]}', {safe_row[9]}, "
                f"'{safe_row[10]}', {safe_row[11]}, {safe_row[12]}, {safe_row[13]}, {safe_row[14]}, "
                f"'{safe_row[15]}', '{safe_row[16]}');"
            )
            rows_to_insert.append(SQL_command)

    logger.info("Total SQL rows generated: %d", len(rows_to_insert))
    return rows_to_insert

def insert_batches_to_redshift(rows, client):
    total = len(rows)
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        logger.info("Inserting batch %d to %d...", i + 1, min(i + BATCH_SIZE, total))
        try:
            response = client.batch_execute_statement(
                WorkgroupName=REDSHIFT_WORKGROUP,
                Database=REDSHIFT_DATABASE,
                Sqls=batch,
                SecretArn=REDSHIFT_SECRET_ARN
            )
            logger.info("Batch insert response: %s", response)
        except Exception as e:
            logger.error("Error during batch insert:\n%s", traceback.format_exc())
