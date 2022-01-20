# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/insurance.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", True) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df.summary().show()

# COMMAND ----------

# MAGIC %pip install plotly

# COMMAND ----------

import pandas as pd
import numpy as np
import statsmodels
import warnings
warnings.filterwarnings('ignore')

import plotly.express as px
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

df_pandas = df.toPandas()
df_pandas = df_pandas.astype({"age": int, "bmi": float, "children" : int,"expenses":float})



# COMMAND ----------

fig = px.histogram(df_pandas, 
                   x='age', 
                   marginal='box', 
                   nbins=20, 
                   title='Distribution of Age')
fig.update_layout(bargap=0.1)
fig.show()

# COMMAND ----------

fig = px.histogram(df_pandas,
                    x='bmi',
                    marginal='box',
                    color_discrete_sequence=['red'],
                    title='Distribustion of BMI (Body Mass Index)')

fig.update_layout(bargap=0.1)
fig.show()

# COMMAND ----------

fig = px.histogram(df_pandas,
                    x='expenses',
                    marginal='box',
                    color='smoker',
                    color_discrete_sequence=['green', 'orange'],
                    title="Annual Medical Expenses")

fig.update_layout(bargap=0.1)
fig.show()                   

# COMMAND ----------

px.histogram(df_pandas, x="smoker", color="sex", title="Smoker")


# COMMAND ----------

fig = px.scatter(df_pandas, x="bmi", y="expenses", color='sex', facet_col="children")
fig.update_xaxes(matches=None)
fig.show()

# COMMAND ----------

df_pandas.dtypes

# COMMAND ----------

try:
    sns.heatmap(df_pandas.corr(), cmap='Reds', annot=True)
except ValueError:  #raised if `y` is empty.
    pass
plt.title('Correlation Matrix')


# COMMAND ----------

# Create a view or table

temp_table_name = "insurance_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `insurance_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "insurance_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
