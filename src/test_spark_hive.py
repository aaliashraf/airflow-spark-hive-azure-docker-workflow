from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


def spark_hive_etl():

    spark = (
        SparkSession.builder.appName("etl_spark_hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .config("spark.sql.warehouse.dir", "/opt/airflow/metastore")
        .enableHiveSupport()
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("Movie", StringType(), True),
            StructField("LeadStudio", StringType(), True),
            StructField("Genre", StringType(), True),
            StructField("Budget", DoubleType(), True),
            StructField("Year", IntegerType(), True),
        ]
    )

    data = [
        ("Spider-Man 3", "Sony", "Action", 258.0, 2007),
        ("Shrek the Third", "Paramount", "Animation", 160.0, 2007),
        ("Transformers", "Paramount", "Action", 150.0, 2007),
        ("Pirates of the Caribbean: At World's End", "Disney", "Action", 300.0, 2007),
        (
            "Harry Potter and the Order of the Phoenix",
            "Warner Bros",
            "Adventure",
            150.0,
            2007,
        ),
    ]

    df = spark.createDataFrame(data, schema)

    table_name = "stats"
    db_name = "movies"

    spark.sql(f"create database if not exists {db_name}")

    df.write.mode("overwrite").partitionBy("LeadStudio").saveAsTable(
        f"{db_name}.{table_name}"
    )

    spark.sql(f"select * from {db_name}.{table_name}").show()

    spark.stop()
