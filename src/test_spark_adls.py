import os
import sys

from pyspark.sql.session import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

adls_account = os.environ.get("ADLS_ACCOUNT")
adls_container = os.environ.get("ADLS_CONTAINER")
adls_acces_key = os.environ.get("ACCESS_KEY")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("etl_spark_azure_data_lake").getOrCreate()

    spark.conf.set(
        f"fs.azure.account.key.{adls_account}.dfs.core.windows.net", adls_acces_key
    )

    adls_url = f"abfss://{adls_container}@{adls_account}.dfs.core.windows.net"

    movies_df = spark.read.parquet(f"{adls_url}/movies_data.parquet")

    movies_df.show()

    movies_df.write.partitionBy("LeadStudio").parquet(
        f"{adls_url}/output", mode="overwrite"
    )
