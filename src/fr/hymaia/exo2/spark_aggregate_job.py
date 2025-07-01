from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def main():
    spark = SparkSession.builder.master("local[*]").appName("spark_aggregate_job").getOrCreate()
    df_clean = spark.read.parquet("data/exo2/clean")
    df_filtre = compter_population_par_departement(df_clean)
    df_filtre.write.option("header", True).mode("overwrite").csv("data/exo2/aggregate")
    df_filtre.show()

def compter_population_par_departement(df):
    return (
        df.groupBy("departement")
        .agg(f.count("*").alias("nb_personnes"))
        .orderBy(f.desc("nb_personnes"), f.asc("departement"))
    )