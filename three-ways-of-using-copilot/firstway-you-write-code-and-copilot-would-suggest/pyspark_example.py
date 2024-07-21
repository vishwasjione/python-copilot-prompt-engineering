# here we want to write pyspark code which will read from aws s3 bucket and create a dataframe
# we also want to filte on state column and get only the rows where state is 'CA'
# and write results back to s3 bucket

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def read_from_s3_bucket(bucket_name: str, file_name: str) -> None:
    spark = SparkSession.builder.appName("Read from S3 bucket").getOrCreate()
    df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True)
    df.filter(col("state") == "CA").write.csv(f"s3a://{bucket_name}/output", mode="overwrite")
    spark.stop()

read_from_s3_bucket("my-bucket", "my-file.csv")

# here I only wrote top 3 lines where I mentioned what I need and then pressed enter and waited for copilot to suggest me the code
# so rest of the lines are suggested by copilot and not written by me