import pymysql
import os

class RDSConnectionManager:

    def lambda_handler(event, context):

        try:
            connection = pymysql.connect(
                host=os.getenv('RDS_HOST'),
                user=os.getenv('RDS_USER'),
                password=os.getenv('RDS_PASSWORD'),
                port=3306
            )
            
            with connection.cursor() as cursor:
                cursor.execute('SELECT %s + %s AS sum', (3, 2))
                result = cursor.fetchone()

            return result
            
        except Exception as e:
            return (f"Error: {str(e)}")  # Return an error message if an exception occurs     