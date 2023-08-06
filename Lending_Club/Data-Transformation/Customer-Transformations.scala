// Databricks notebook source
// MAGIC %md #Scenarios Covered

// COMMAND ----------

// MAGIC %md
// MAGIC * Count of total customers, grouped by state and country.
// MAGIC * Count of premium customers, grouped by state and country.
// MAGIC * Percentage of premium customers within each state, grouped by country.
// MAGIC * Average age of customers, grouped by state and country.

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Query to find the total count of customers by state and country

// COMMAND ----------

spark.sql("""SELECT state, country, count(*) as total_count
FROM work.customer_details
GROUP BY state, country """).show();


// COMMAND ----------

// MAGIC %md
// MAGIC ##### Query to find the count of customers having premium membership falling in different age buckets

// COMMAND ----------

spark.sql("""SELECT country, 
                      CASE 
                          WHEN age BETWEEN 18 AND 25 THEN 'Youngsters'
                          WHEN age BETWEEN 26 AND 35 THEN 'Working class'
                          WHEN age BETWEEN 36 AND 45 THEN 'Middle Age'
                          ELSE 'Senior Citizens'
                      END as age_range,
                      COUNT(*) as total_members
              FROM work.customer_details
              WHERE premium_status = 'TRUE'
              GROUP BY country, age_range """).show()


// COMMAND ----------

//Query to find the number of customers with a premium status of "true" in each country, grouped by age range using scala:
val customersDF = spark.sql("""select * from work.customer_details""")
customersDF.filter("premium_status = 'TRUE'" ) 
           .withColumn("age_range", when((col("age") >= 18) && (col("age") <= 25), "Youngsters")
                                    .when((col("age") > 25) && (col("age") <= 35), "Working class")
                                    .when((col("age") > 35) && (col("age") <= 45), "Middle Age")
                                    .otherwise("Senior citizens")) 
           .groupBy("country", "age_range") 
           .agg(count("*").alias("total_members"))
           .show()

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS cooked;

// COMMAND ----------

// MAGIC %md
// MAGIC #####Query to find the percentage of customers in each state that are premium customers, grouped by country

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS cooked.customers_premium_status 
// MAGIC USING PARQUET
// MAGIC LOCATION '/mnt/lendingClub/cooked/lendingloan/customer-transformations/customers_premium_status'
// MAGIC
// MAGIC WITH customer_count AS (
// MAGIC     SELECT country, state, COUNT(*) as total_customers_count
// MAGIC     FROM work.customer_details
// MAGIC     GROUP BY country, state
// MAGIC ),
// MAGIC premimum_member_count AS (
// MAGIC     SELECT country, state, COUNT(DISTINCT member_id) as total_premium_members_count
// MAGIC     FROM work.customer_details
// MAGIC     WHERE member_id IS NOT NULL and  premium_status = 'TRUE'
// MAGIC     GROUP BY country, state
// MAGIC )
// MAGIC SELECT 
// MAGIC     CC.country, 
// MAGIC     CC.state, 
// MAGIC     ROUND(PMC.total_premium_members_count / CC.total_customers_count * 100, 2) as percent_of_premium_members
// MAGIC FROM 
// MAGIC     customer_count CC
// MAGIC JOIN 
// MAGIC     premimum_member_count PMC
// MAGIC ON 
// MAGIC     CC.country = PMC.country AND CC.state = PMC.state;
// MAGIC

// COMMAND ----------

// MAGIC %md #####Query to find the average age of customers by state and country using Scala dataframe

// COMMAND ----------

val customersAvgAge=customersDF.groupBy("state", "country")
                              .agg(format_number(avg("age"), 2).alias("average_age"))
customersAvgAge.createOrReplaceTempView("customersAvgAge")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS cooked.customers_avg_age 
// MAGIC USING PARQUET
// MAGIC LOCATION '/mnt/lendingClub/cooked/lendingloan/customer-transformations/customers_avg_age'
// MAGIC select * from customersAvgAge

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM cooked.customers_avg_age

// COMMAND ----------

dbutils.notebook.run("Lending_Club/Data-Transformation/loan-score-customers", timeoutSeconds = 600)
