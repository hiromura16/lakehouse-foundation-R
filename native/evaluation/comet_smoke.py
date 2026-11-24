"""Sparkの集計結果と物理プランを照合してComet実行を確認する。"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Foundation R Comet").getOrCreate()
try:
    # Parquetを介してCometのスキャンと集計を検査する。
    path = "/tmp/foundation-comet-parquet"
    spark.range(1, 101).write.mode("error").parquet(path)
    frame = spark.read.parquet(path).selectExpr("sum(id) AS total")
    assert frame.collect()[0]["total"] == 5050
    physical_plan = frame._jdf.queryExecution().executedPlan().toString()
    assert "Comet" in physical_plan, physical_plan
    print("Cometの物理プランを確認しました。合計 5050")
finally:
    spark.stop()
