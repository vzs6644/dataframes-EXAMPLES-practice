#assignment - to read a json to mongo db

from pyspark.sql import SparkSession
import yaml
import os.path
from model.Student import Student

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.2" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .config("spark.mongodb.output.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')


    # question ? in the below line we created a dataframe named "students". the name of the collection inside the momgo
    # database named "school" is "students"!! is this compulsary or can we give any name we want ???
    # lets test this!!!!! ask someone

    students = spark.createDataFrame(
    spark.sparkContext.parallelize(
        # [Student("Sidhartha", "Ray", "ITER", 200), Student("Satabdi", "Ray", "CET", 100)]))
        [Student("Sidhartha122312", "Ray2323", "ITERd23232", 203310), Student("Satabdiw2323", "Raysdsd", "CET34234", 105530)])) #my edit

    students.show()

    students\
        .write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("database", app_conf["mongodb_config"]["database"])\
        .option("collection", app_conf["mongodb_config"]["collection"])\
        .save()

# spark-submit --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.2" dataframe/provision/json_df_mongo.py
