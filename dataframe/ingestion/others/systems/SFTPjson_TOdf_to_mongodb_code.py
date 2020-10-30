# code to read a file in sftp to a dataframe DF in spark


from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .master('local[*]') \
        .config('spark.jars.packages', 'com.springml:spark-sftp_2.11:1.1.1') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    ol_txn_df = spark.read\
        .format("com.springml.spark.sftp")\
        .option("host", app_secret["sftp_conf"]["hostname"])\
        .option("port", app_secret["sftp_conf"]["port"])\
        .option("username", app_secret["sftp_conf"]["username"])\
        .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"]))\
        .option("fileType", "json")\
        .load(app_conf["sftp_conf"]["directory"] + "/KC_Extract_2_20171009.json")

    ol_txn_df.show(5, False)




# companies_df = spark.read.json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/company.json")


# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1" dataframe/ingestion/others/systems/SFTPjson_TOdf_to_mongodb_code.py

