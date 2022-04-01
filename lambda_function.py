import urllib.parse
import boto3
import redshift_connector

conn_read = redshift_connector.connect(database='database',
                                       host='host',
                                       port=5439,
                                       user='user',
                                       password='password'
                                       )
print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        blob_path = f's3://{bucket}/{key}'
        print(f'blob_path: {blob_path}')
        query = f"""
        copy category from '{blob_path}'
        iam_role 'arn:aws:iam::account_id:role/role_name'
        region 'eu-west-1'
        CSV DELIMITER ','
        IGNOREHEADER 1
        """
    
        conn_read.autocommit = True
        cursor_read = conn_read.cursor()
        cursor_read.execute(query)
        return True

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
