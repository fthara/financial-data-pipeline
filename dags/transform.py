from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lag, lit
from pyspark.sql.window import Window

BASE_PATH = Path("/app/data/financial_data_pipeline")
PRICE_PATH = BASE_PATH / "bronze/prices"
DIV_PATH = BASE_PATH / "bronze/dividends"
SPLIT_PATH = BASE_PATH / "bronze/splits"

spark = (
    SparkSession.builder
    .appName("financial-pipeline")
    .master("local[*]")
    .getOrCreate()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def transform_data():
    df_price = (
        spark.read.parquet(str(PRICE_PATH))
        .drop("Dividends", "Stock Splits")
        .withColumnsRenamed({
            "Date": "date",
            "Ticker": "ticker",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })
    )
    df_divs = (
        spark.read.parquet(str(DIV_PATH))
        .withColumnsRenamed({
            "Date": "date",
            "Ticker": "ticker",
            "Dividends": "dividends"
        })
    )
    df_splits = (
        spark.read.parquet(str(SPLIT_PATH))
        .withColumnsRenamed({
            "Date": "date",
            "Ticker": "ticker",
            "Stock Splits": "splits"
        })
    )

    df = (
        df_price
        .join(df_divs, on=["date", "ticker"], how="left")
        .join(df_splits, on=["date", "ticker"], how="left")
    )

    df = (
        df.withColumn("dividends", coalesce("dividends", lit(0.0)))
        .withColumn("splits", coalesce("splits", lit(1.0)))
    )

    window = Window.partitionBy("ticker").orderBy("date")

    df = df.withColumn( # Retorno diario
        "return",
        (col("close") - lag("close").over(window)) / lag("close").over(window)
    )

    df.write.mode("overwrite").partitionBy("dt_date").parquet(str(BASE_PATH / "silver/combined/"))


if __name__=="__main__":
    transform_data()