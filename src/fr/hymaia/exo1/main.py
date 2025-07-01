import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame


def main():
    spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    df = spark.read.option("header", True).csv("src/resources/exo1/data.csv")
    count: DataFrame = wordcount(df, "text")
    count.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")



def wordcount(df, col_name) -> DataFrame:
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
