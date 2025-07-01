from xml.dom.minicompat import StringTypes
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame


def main():
    spark = SparkSession.builder.master("local[*]").appName("spark_clean_job").getOrCreate()
    df_city = spark.read.option("header", True).csv("src/resources/exo2/city_zipcode.csv")
    df_client = spark.read.option("header", True).csv("src/resources/exo2/clients_bdd.csv")
    df_filtered = filter_age(df_client)
    df_joined = joindre(df_filtered, df_city)
    df_final = ajouter_departement(df_joined)
    df_final.write.mode("overwrite").parquet("data/exo2/clean")

def joindre(df_client, df_city) -> DataFrame:
    return df_client.join(df_city, "zip", "inner")

def filter_age(df_client) -> DataFrame:
    return df_client.filter(f.col("age") >= 18)

def get_depart(zipcode):
    if zipcode.startswith("20") and int(zipcode) <= 20190:
        return "2A"
    elif zipcode.startswith("20"):
        return "2B"
    return zipcode[:2]
get_dept_udf = f.udf(get_depart)

def ajouter_departement(df_client: DataFrame) -> DataFrame:
    return df_client.withColumn("departement", 
                                f.when((f.col("zip").startswith("20") & (f.col("zip") <= "20190")), 
                                       f.lit("2A"))
                                .when((f.col("zip").startswith("20") & (f.col("zip") > "20190")), 
                                       f.lit("2B"))
                                .otherwise(f.col("zip").substr(1,2))
                                )

def ajouter_departement2(df_client: DataFrame) -> DataFrame:
    return df_client.withColumn("departement", 
                                f.when(~f.col("zip").startswith("20"), f.col("zip").substr(0,2) ) 
                                .when(f.col("zip") <= 20190, f.lit("2A"))     
                                .otherwise(f.lit("2B"))
                                )