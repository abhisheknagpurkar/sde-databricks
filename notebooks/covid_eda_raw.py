# Databricks notebook source
# MAGIC %fs mkdirs tmp

# COMMAND ----------

# MAGIC %fs
# MAGIC wget -q https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv -O /tmp/covid-hospitalizations.csv

# COMMAND ----------

dbutils.fs.cp("file:/tmp/covid-hospitalizations.csv", "dbfs:/tmp/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/tmp/")

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = pd.read_csv("/tmp/covid-hospitalizations.csv")
df = (df[df.iso_code == 'USA']
      .pivot_table(values="value", columns="indicator", index="date")
      .fillna(0))
display(df)

# COMMAND ----------

df.plot(figsize=(13,6), grid=True).legend(loc="upper right")

# COMMAND ----------

import pyspark.pandas as ps

clean_cols = df.columns.str.replace(' ', "_")
psdf = ps.from_pandas(df)
psdf.columns = clean_cols
psdf['date'] = psdf.index
display(psdf)

# COMMAND ----------

psdf.to_table(name="dev_covid_analysis", mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_covid_analysis

# COMMAND ----------


