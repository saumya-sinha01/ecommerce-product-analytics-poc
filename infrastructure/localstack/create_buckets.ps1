$env:AWS_ACCESS_KEY_ID="test"
$env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"

$ENDPOINT="http://localhost:4566"

aws --endpoint-url=$ENDPOINT s3 mb s3://ecom-poc-raw 2>$null
aws --endpoint-url=$ENDPOINT s3 mb s3://ecom-poc-processed 2>$null
aws --endpoint-url=$ENDPOINT s3 ls
