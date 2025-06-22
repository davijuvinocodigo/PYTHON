#!/bin/bash

# Variáveis (substitua pelos seus valores)
BUCKET_NAME="meu-bucket-s3"
LAMBDA_ARN="arn:aws:lambda:us-east-1:123456789012:function:minha-funcao-lambda"
AWS_REGION="us-east-1"
PREFIX="entrada/"
SUFFIX=".csv"

# Adiciona permissão para o S3 invocar a Lambda
aws lambda add-permission \
    --function-name $LAMBDA_ARN \
    --principal s3.amazonaws.com \
    --statement-id "s3-trigger-$BUCKET_NAME" \
    --action "lambda:InvokeFunction" \
    --source-arn "arn:aws:s3:::$BUCKET_NAME" \
    --source-account $(aws sts get-caller-identity --query Account --output text) \
    --region $AWS_REGION

# Configura a notificação do bucket S3
aws s3api put-bucket-notification-configuration \
    --bucket $BUCKET_NAME \
    --notification-configuration '{
        "LambdaFunctionConfigurations": [
            {
                "LambdaFunctionArn": "'$LAMBDA_ARN'",
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": "'$PREFIX'"
                            },
                            {
                                "Name": "suffix",
                                "Value": "'$SUFFIX'"
                            }
                        ]
                    }
                }
            }
        ]
    }' \
    --region $AWS_REGION

echo "Gatilho S3 configurado automaticamente para a Lambda $LAMBDA_ARN"