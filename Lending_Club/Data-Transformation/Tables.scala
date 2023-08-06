// Databricks notebook source
// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS work;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Create External Hive tables

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS work.account_details (
// MAGIC   account_key STRING,
// MAGIC   account_id STRING,
// MAGIC   member_id STRING,
// MAGIC   loan_id STRING,
// MAGIC   grade STRING,
// MAGIC   sub_grade STRING,
// MAGIC   employee_designation STRING,
// MAGIC   employee_experience INT,
// MAGIC   home_ownership STRING,
// MAGIC   annual_income FLOAT,
// MAGIC   verification_status STRING,
// MAGIC   total_high_credit_limit FLOAT,
// MAGIC   application_type STRING,
// MAGIC   annual_income_joint STRING,
// MAGIC   verification_status_joint STRING,
// MAGIC   run_date DATE)
// MAGIC USING PARQUET
// MAGIC PARTITIONED BY (run_date)
// MAGIC LOCATION 'dbfs:/mnt/lendingClub/work/lendingloan/ACCOUNT_DETAILS'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS work.customer_details (
// MAGIC   customer_key STRING,
// MAGIC   customer_id STRING,
// MAGIC   member_id STRING,
// MAGIC   first_name STRING,
// MAGIC   last_name STRING,
// MAGIC   premium_status STRING,
// MAGIC   age INT,
// MAGIC   state STRING,
// MAGIC   country STRING,
// MAGIC   run_date DATE)
// MAGIC USING PARQUET
// MAGIC PARTITIONED BY (run_date)
// MAGIC LOCATION 'dbfs:/mnt/lendingClub/work/lendingloan/CUSTOMER_DETAILS'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS work.defaulter_details (
// MAGIC   loan_default_key STRING,
// MAGIC   loan_id STRING,
// MAGIC   member_id STRING,
// MAGIC   loan_default_id STRING,
// MAGIC   defaulters_2yrs INT,
// MAGIC   defaulters_amount FLOAT,
// MAGIC   public_records INT,
// MAGIC   public_records_bankruptcies INT,
// MAGIC   enquiries_6mnths INT,
// MAGIC   late_fee FLOAT,
// MAGIC   hardship_flag STRING,
// MAGIC   hardship_type STRING,
// MAGIC   hardship_length INT,
// MAGIC   hardship_amount FLOAT,
// MAGIC   run_date DATE)
// MAGIC USING PARQUET
// MAGIC PARTITIONED BY (run_date)
// MAGIC LOCATION 'dbfs:/mnt/lendingClub/work/lendingloan/DEFAULTER_DETAILS'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS work.investor_details (
// MAGIC   investor_loan_key STRING,
// MAGIC   investor_loan_id STRING,
// MAGIC   loan_id STRING,
// MAGIC   investor_id STRING,
// MAGIC   loan_funded_amt DOUBLE,
// MAGIC   investor_type STRING,
// MAGIC   age INT,
// MAGIC   state STRING,
// MAGIC   country STRING,
// MAGIC   run_date DATE)
// MAGIC USING PARQUET
// MAGIC PARTITIONED BY (run_date)
// MAGIC LOCATION 'dbfs:/mnt/lendingClub/work/lendingloan/INVESTOR_DETAILS'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS work.loan_details (
// MAGIC   loan_key STRING,
// MAGIC   loan_id STRING,
// MAGIC   member_id STRING,
// MAGIC   account_id STRING,
// MAGIC   loan_amount DOUBLE,
// MAGIC   funded_amount DOUBLE,
// MAGIC   term STRING,
// MAGIC   interest STRING,
// MAGIC   installment FLOAT,
// MAGIC   issue_date DATE,
// MAGIC   loan_status STRING,
// MAGIC   purpose STRING,
// MAGIC   title STRING,
// MAGIC   disbursement_method STRING,
// MAGIC   run_date DATE)
// MAGIC USING PARQUET
// MAGIC PARTITIONED BY (run_date)
// MAGIC LOCATION 'dbfs:/mnt/lendingClub/work/lendingloan/LOAN_DETAILS'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS work.payment_details (
// MAGIC   payment_key STRING,
// MAGIC   loan_id STRING,
// MAGIC   member_id STRING,
// MAGIC   latest_transaction_id STRING,
// MAGIC   funded_amount_investor DOUBLE,
// MAGIC   total_payment_recorded FLOAT,
// MAGIC   installment FLOAT,
// MAGIC   last_payment_amount FLOAT,
// MAGIC   last_payment_date DATE,
// MAGIC   next_payment_date DATE,
// MAGIC   payment_method STRING,
// MAGIC   run_date DATE)
// MAGIC USING PARQUET
// MAGIC PARTITIONED BY (run_date)
// MAGIC LOCATION 'dbfs:/mnt/lendingClub/work/lendingloan/PAYMENT_DETAILS'

// COMMAND ----------

dbutils.notebook.run("Lending_Club/Data-Transformation/Customer-Transformations", timeoutSeconds = 600)
