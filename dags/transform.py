from pathlib import Path
from pyspark.sql import SparkSession

BASE_PATH = Path("/home/fernando/fernando/projects/data/financial_data_pipeline/features")
PRICE_PATH = BASE_PATH / "prices"
DIV_PATH = BASE_PATH / "dividends"
SPLIT_PATH = BASE_PATH / "splits"

spark = (
    SparkSession.builder
    .appName("financial-pipeline")
    .master("local[*]")
    .getOrCreate()
)


def transform_data():
    df_price = spark.read.parquet(str(PRICE_PATH))
    print(df_price.show())

    df_divs = spark.read.parquet(str(DIV_PATH))
    print(df_divs.show())

    df_splits = spark.read.parquet(str(SPLIT_PATH))
    print(df_splits.show())


if __name__=="__main__":
    transform_data()