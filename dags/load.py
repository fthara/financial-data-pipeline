from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, exp, lag, log, max, rank, sum as _sum, stddev
from pyspark.sql.window import Window

BASE_PATH = Path("/app/data/financial_data_pipeline")
COMBINED_PATH = BASE_PATH / "silver/combined"

spark = (
    SparkSession.builder
    .appName("financial-pipeline")
    .master("local[*]")
    .getOrCreate()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def performance():
    df = (
        spark.read.parquet(str(COMBINED_PATH))
        .select("date", "ticker", "close", "dividends", "splits", "return", "dt_date")
    )

    window = Window.partitionBy("ticker").orderBy("date")

    df = (
        df.withColumn( # Retorno com dividendos
            "total_return",
            (col("close") + col("dividends") - lag("close").over(window)) /
            lag("close").over(window)
        ).withColumn("log_splits", log(col("splits"))) # Fator acumulado
        .withColumn("cum_split_factor", exp(_sum("log_splits").over(window)))
    )

    df = df.select(
        "date", "ticker", "return", "total_return", "cum_split_factor", "dt_date"
    )

    df.write.mode("overwrite").partitionBy("dt_date").parquet(str(BASE_PATH / "gold/performance/"))


def indicators():
    df = (
        spark.read.parquet(str(COMBINED_PATH))
        .select("date", "ticker", "close", "return", "dt_date")
    )

    window_7 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)

    df = (
        df.withColumn("ma_7", avg("close").over(window_7)) # Média Móvel
        .withColumn("volatility_7", stddev("return").over(window_7)) # Volatilidade
    )

    window_all = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    df = ( # dradown
        df.withColumn("max_price", max("close").over(window_all))
        .withColumn("drawdown", (col("close") - col("max_price")) / col("max_price"))
    )

    df = df.select("date", "ticker", "ma_7", "volatility_7", "drawdown", "dt_date")

    df.write.mode("overwrite").partitionBy("dt_date").parquet(str(BASE_PATH / "gold/indicators/"))

def ranking():
    df = (
        spark.read.parquet(str(COMBINED_PATH))
        .select("date", "ticker", "return", "dt_date")
    )

    window_rank = Window.partitionBy("date").orderBy(col("return").desc())

    df = df.withColumn("rank", rank().over(window_rank)) # ranking diario

    df = df.select("date", "ticker", "rank", "dt_date")

    df.write.mode("overwrite").partitionBy("dt_date").parquet(str(BASE_PATH / "gold/ranking/"))


if __name__=="__main__":
    performance()
    indicators()
    ranking()