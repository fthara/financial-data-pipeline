from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, col, exp, lag, lit, log, max, rank, sum as _sum, stddev
from pyspark.sql.window import Window

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

    df = (
        df.withColumn( # Retorno diario
            "return",
            (col("close") - lag("close").over(window)) / lag("close").over(window)
        ).withColumn( # Retorno com dividendos
            "total_return",
            (col("close") + col("dividends") - lag("close").over(window)) /
            lag("close").over(window)
        ).withColumn("log_splits", log(col("splits"))) # Fator acumulado
        .withColumn("cum_split_factor", exp(_sum("log_splits").over(window)))
    )

    window_7 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)

    df = (
        df.withColumn("ma_7", avg("close").over(window_7)) # Média Móvel
        .withColumn("volatility_7", stddev("return").over(window_7)) # Volatilidade
    )

    window_all = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    df = ( # dradown
        df.withColumn("max_price", max("close").over(window_all))
        .withColumn("drawdown", (col("close") - col("max_price") / col("max_price")))
    )

    window_rank = Window.partitionBy("date").orderBy(col("return").desc())

    df = df.withColumn("rank", rank().over(window_rank)) # ranking diario

    print(df.show())
    print(df.printSchema())


if __name__=="__main__":
    transform_data()