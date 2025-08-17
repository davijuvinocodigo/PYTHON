from chalice import Chalice
from chalicelib.lambda_function import lambda_handler
from chalicelib.core.logger import log
import json
app = Chalice(app_name='lambdaS3-RDS')

@app.lambda_function()
def call_function(event, context):
    result = lambda_handler(event, context)
    log.info(json.dumps(result))
