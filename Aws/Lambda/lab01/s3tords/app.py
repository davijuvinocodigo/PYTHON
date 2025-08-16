from chalice import Chalice
from chalicelib.lambda_function import lambda_handler


app = Chalice(app_name='s3tords')


@app.lambda_function()
def call_function(event, context):
    lambda_handler(event, context)
#
