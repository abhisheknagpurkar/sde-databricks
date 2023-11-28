# Databricks notebook source
# MAGIC %load_ext autoreload

# COMMAND ----------

# MAGIC %autoreload 2

# COMMAND ----------

data_path = "/tmp/covid-hospitalizations.csv"
print(f"Data Path: {data_path}")

# COMMAND ----------

import pandas as pd
from covid_analysis.transforms import *

df = pd.read_csv(data_path)
df = filter_country(df, country='DZA')
df = pivot_and_clean(df, fillna=0)
df = clean_spark_cols(df)
df = index_to_col(df, colname='date')

# COMMAND ----------

df = spark.createDataFrame(df)
display(df)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('covid_stats')

# COMMAND ----------

display(spark.table('covid_stats'))

# COMMAND ----------

df.toPandas().plot(figsize=(13, 6), grid=True).legend(loc="upper right")

# COMMAND ----------


