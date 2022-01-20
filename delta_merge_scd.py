# Databricks notebook source
# MAGIC %scala
# MAGIC import java.sql.Date
# MAGIC import java.text._
# MAGIC import spark.implicits
# MAGIC  
# MAGIC case class CustomerUpdate(customerId: Int, address: String, effectiveDate: Date)
# MAGIC case class Customer(customerId: Int, address: String, current: Boolean, effectiveDate: Date, endDate: Date)
# MAGIC  
# MAGIC implicit def date(str: String): Date = Date.valueOf(str)
# MAGIC  
# MAGIC sql("drop table if exists customers")
# MAGIC  
# MAGIC Seq(
# MAGIC   Customer(1, "old address for 1", false, null, "2018-02-01"),
# MAGIC   Customer(1, "current address for 1", true, "2018-02-01", null),
# MAGIC   Customer(2, "current address for 2", true, "2018-02-01", null),
# MAGIC   Customer(3, "current address for 3", true, "2018-02-01", null)
# MAGIC ).toDF().write.format("delta").mode("overwrite").saveAsTable("customers")
# MAGIC  
# MAGIC display(table("customers").orderBy("customerId"))

# COMMAND ----------

# MAGIC %scala
# MAGIC Seq(
# MAGIC   CustomerUpdate(1, "new address for 1", "2018-03-03"),
# MAGIC   CustomerUpdate(3, "current address for 3", "2018-04-04"),    // new address same as current address for customer 3
# MAGIC   CustomerUpdate(4, "new address for 4", "2018-04-04")
# MAGIC ).toDF().createOrReplaceTempView("updates")
# MAGIC display(table("updates"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC  
# MAGIC -- ========================================
# MAGIC -- Merge SQL API is available since DBR 5.1
# MAGIC -- ========================================
# MAGIC  
# MAGIC MERGE INTO customers
# MAGIC USING (
# MAGIC    -- These rows will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
# MAGIC   SELECT updates.customerId as mergeKey, updates.*
# MAGIC   FROM updates
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- These rows will INSERT new addresses of existing customers 
# MAGIC   -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
# MAGIC   SELECT NULL as mergeKey, updates.*
# MAGIC   FROM updates JOIN customers
# MAGIC   ON updates.customerid = customers.customerid 
# MAGIC   WHERE customers.current = true AND updates.address <> customers.address 
# MAGIC   
# MAGIC ) staged_updates
# MAGIC ON customers.customerId = mergeKey
# MAGIC WHEN MATCHED AND customers.current = true AND customers.address <> staged_updates.address THEN  
# MAGIC   UPDATE SET current = false, endDate = staged_updates.effectiveDate    -- Set current to false and endDate to source's effective date.
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT(customerid, address, current, effectivedate, enddate) 
# MAGIC   VALUES(staged_updates.customerId, staged_updates.address, true, staged_updates.effectiveDate, null) -- Set current to true along with the new address and its effective date.
# MAGIC  

# COMMAND ----------

# MAGIC %scala
# MAGIC // ==========================================
# MAGIC // Merge Scala API is available since DBR 6.0
# MAGIC // ==========================================
# MAGIC  
# MAGIC import io.delta.tables._
# MAGIC  
# MAGIC val customersTable: DeltaTable =   // table with schema (customerId, address, current, effectiveDate, endDate)
# MAGIC   DeltaTable.forName("customers")
# MAGIC  
# MAGIC val updatesDF = table("updates")          // DataFrame with schema (customerId, address, effectiveDate)
# MAGIC  
# MAGIC // Rows to INSERT new addresses of existing customers
# MAGIC val newAddressesToInsert = updatesDF
# MAGIC   .as("updates")
# MAGIC   .join(customersTable.toDF.as("customers"), "customerid")
# MAGIC   .where("customers.current = true AND updates.address <> customers.address")
# MAGIC  
# MAGIC // Stage the update by unioning two sets of rows
# MAGIC // 1. Rows that will be inserted in the `whenNotMatched` clause
# MAGIC // 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
# MAGIC val stagedUpdates = newAddressesToInsert
# MAGIC   .selectExpr("NULL as mergeKey", "updates.*")   // Rows for 1.
# MAGIC   .union(
# MAGIC     updatesDF.selectExpr("updates.customerId as mergeKey", "*")  // Rows for 2.
# MAGIC   )
# MAGIC  
# MAGIC // Apply SCD Type 2 operation using merge
# MAGIC customersTable
# MAGIC   .as("customers")
# MAGIC   .merge(
# MAGIC     stagedUpdates.as("staged_updates"),
# MAGIC     "customers.customerId = mergeKey")
# MAGIC   .whenMatched("customers.current = true AND customers.address <> staged_updates.address")
# MAGIC   .updateExpr(Map(                                      // Set current to false and endDate to source's effective date.
# MAGIC     "current" -> "false",
# MAGIC     "endDate" -> "staged_updates.effectiveDate"))
# MAGIC   .whenNotMatched()
# MAGIC   .insertExpr(Map(
# MAGIC     "customerid" -> "staged_updates.customerId",
# MAGIC     "address" -> "staged_updates.address",
# MAGIC     "current" -> "true",
# MAGIC     "effectiveDate" -> "staged_updates.effectiveDate",  // Set current to true along with the new address and its effective date.
# MAGIC     "endDate" -> "null"))
# MAGIC   .execute()

# COMMAND ----------

display(table("customers").orderBy("customerId", "current", "endDate"))

