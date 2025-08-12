from chalice import Chalice
from chalicelib.database import RDSConnectionManager
import os

app = Chalice(app_name='s3-csv-to-rds')
app.debug = True


@app.on_s3_event(bucket='dev-bucket-lab01',
                 events=['s3:ObjectCreated:*'],
                 prefix='entrada/')
def handle_s3_event(event):
    RDSConnectionManager.lambda_handler(event, None)