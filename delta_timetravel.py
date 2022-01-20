# Databricks notebook source
## feature branch code
import os, sys, time
sys.version
os.environ.get('PYSPARK_SUBMIT_ARGS',None)
in_databricks = 'DATABRICKS_RUNTIME_VERSION' in os.environ
in_databricks

# COMMAND ----------

table = "cats"
if in_databricks:
  user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
  user = user.split("@")[0].replace(".","_")
  database = f"{user}_delta_fun"
  dataPath = f"dbfs:/tmp/{user}/delta_fun/table_cats"
else:
  database = "delta_fun"
  dataPath = "delta_fun"
database,dataPath

# COMMAND ----------

def now(): 
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
now()

# COMMAND ----------

def insert(path, data, mode="append"):
    df = spark.createDataFrame(data, ["id","name","region"])
    df.coalesce(1).write.mode(mode).format("delta").save(path)

# COMMAND ----------

insert(dataPath, [
    (1, "lion","africa"),
    (2, "cheetah","africa"),
    (3, "leopard","africa")], "overwrite")
insert(dataPath, [
    (4, "jaguar","south america"),
    (5, "puma","south america"),
    (6, "ocelot","south america")])
insert(dataPath, [
    (7, "lynx","north america"),
    (8, "bobcat","north america"),
    (9, "catamount","north america")])

# COMMAND ----------

if in_databricks:
  for f in dbutils.fs.ls(dataPath):
    print(f.size,f.name)
else:
  for f in os.listdir(dataPath):
    print(f)

# COMMAND ----------

df = spark.read.format("delta").load(dataPath)
df.sort("id").show()

# COMMAND ----------

for v in range(0,3):
    print("Version",v)
    df = spark.read.format("delta").option("versionAsOf", str(v)).load(dataPath)
    df.sort("id").show(100,False)

# COMMAND ----------

spark.sql(f"create table {table} using delta location '{dataPath}'")
spark.sql(f"describe formatted {table}").show(1000,False)
spark.sql(f"select * from {table} order by id").show()


# COMMAND ----------

if in_databricks: spark.sql(f"describe history {table}").show()
if in_databricks: spark.sql(f"describe detail {table}").show()
if in_databricks: 
    for v in range(0,3):
        print("Version",v)
        spark.sql(f"select * from {table} version as of {v} order by id").show()

# COMMAND ----------

now()
