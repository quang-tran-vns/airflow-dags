from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("TranslateThaiToEnglish") \
    .getOrCreate()


INPUT_PATH = "/opt/airflow/data/input/thai_data.csv"
MAPPING_PATH = "/opt/airflow/data/mapping/mapping_table.csv"
OUTPUT_PATH = "/opt/airflow/data/output/products_en"


df_products = spark.read.option("header", True).csv(INPUT_PATH)
df_mapping = spark.read.option("header", True).csv(MAPPING_PATH)


df_products = df_products.join(
    df_mapping.withColumnRenamed("english_value", "product_name_en"),
    df_products.product_name == df_mapping.thai_value,
    how="left"
).drop("thai_value")


df_products = df_products.join(
    df_mapping.withColumnRenamed("english_value", "category_en"),
    df_products.category == df_mapping.thai_value,
    how="left"
).drop("thai_value")


df_final = df_products.select(
    col("product_name_en").alias("product_name"),
    col("category_en").alias("category"),
    col("price").cast("int")
)


df_final.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_PATH)

spark.stop()
