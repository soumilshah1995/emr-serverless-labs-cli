# emr-serverless-labs-cli
emr-serverless-labs-cli
![Capture](https://github.com/soumilshah1995/emr-serverless-labs-cli/assets/39345855/fe237a3d-bcef-4d1a-8cbd-5d7d96df55cb)

#### Command
```
Step 1:
pip3 install emr-cli

Step 2:
    setx ACCESS_KEY "XXX"
    setx SECRET_KEY "XXXX"
    OR
    aws configure

Step 3:
emr init my-project

Step 4:
emr run `
    --entry-point entrypoint.py `
    --application-id <application id> `
    --job-role <ARN> `
    --s3-code-uri s3://jt-soumilshah-test/emr_scripts/ `
    --spark-submit-opts "--conf spark.jars=/usr/lib/hudi/hudi-spark-bundle.jar --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory" `
    --build `
    --wait

```
