# Lending_Club

## Project Background
*	Lending Club is a well-known peer-to-peer lending firm headquartered in San Francisco, California.
* It connects borrowers with good credit scores and established financial histories with potential lenders.
*	Lenders can decide whether to lend money based on the borrower's credit score and income data.
*	Lending Club offers personal loans with interest rates ranging from 6% to 36%, with repayment schedules of 36 to 60 months.
*	Pros of Lending Club include less paperwork, long loan terms, and easy access to personal loans.
*	Cons of Lending Club include high-interest rates, origination fees, and risks for investors.
Problem Statement:
*	The project aims to provide insights from customer data to identify trusted borrowers, calculate credit scores, and analyze loan availing patterns to improve the business.

## **Problem Statement**
* The project aims to provide insights from customer data to identify trusted borrowers, calculate credit scores, and analyze loan availing patterns to improve the business.


## Project Infrastructure Setup
*	Azure Databricks will be used to implement the project. Databricks clusters such as All-Purpose Clusters, Job Clusters, and Cluster Pools will be created.
*	Data will be stored in Azure Blob Storage and Azure Data Lake Storage (ADLS Gen2).
*	Data Ingestion will be performed using Azure Data Lake Storage and Azure Data Factory.
*	Azure Key Vault will be used to securely store credentials.

## Data Sources
### Customer Table (customer):
  * Contains basic details of borrowers, such as Cust_Id, Mem_Id, First_Name, Last_Name, Premium_Status, Age, State, and Country.

| Column Name | Data Type | Description                          |
|-------------|-----------|--------------------------------------|
| Cust_Id     | IntegerType      | Customer ID                          |
| Mem_Id      | StringType    | Member ID from the Lending Club Platform |
| First_Name  | StringType    | First name of the customer           |
| Last_Name   | StringType    | Last name of the customer            |
| Prm_Status  | BooleanType   | Premium membership status            |
| Age         | IntegerType      | Age of the customer                   |
| State       | StringType    | State where the customer resides     |
| Country     | StringType    | Country where the customer resides   |

### Loan Status Table (loan_status):
  * Contains details regarding loans, including Loan_Id, Mem_Id, Loan_Amount, Fund_Amount, Term, Interest Rate, Installment Amount, Issue DateType, and Loan Status.

| Column Name       | Data Type | Description                                     |
|-------------------|-----------|-------------------------------------------------|
| Loan_Id           | IntegerType      | Loan ID                                         |
| Mem_Id            | StringType    | Member ID from the Lending Club Platform        |
| Loan_amt          | FloatType    | Requested loan amount                           |
| Fund_amt          | FloatType    | Amount funded by investors                      |
| Term              | IntegerType      | Repayment term in months                        |
| Interest_Rate     | FloatType    | Interest rate on the loan                       |
| Installment_Amount| FloatType    | Installment amount to be paid                   |
| Issue_Date        |DateType     | Date when the loan was issued                   |
| Loan_Status       | StringType    | Status of the loan (Current, Charged Off, Fully Paid, etc.) |
| Purpose           | StringType    | Category provided by the borrower for the loan request |
| Title             | StringType    | Title provided by the borrower for the loan request |
| Disbursement_Method | StringType  | Mode of payment for the loan (Cash, Direct Pay, etc.) |

### Account Table (account):
 * Contains borrower account details, including Acc_Id, Mem_Id, Grade, Sub_Grade, Emp_Title, Emp_Length, Home_Ownership, Annual_Income, and more.

| Column Name           | Data Type | Description                                        |
|-----------------------|-----------|----------------------------------------------------|
| Acc_Id                | IntegerType      | Account ID                                         |
| Mem_Id                | StringType    | Member ID from the Lending Club Platform           |
| Grade                 | CharType     | Grade assigned based on customer's details         |
| Sub_Grade             | CharType     | Sub-grade assigned based on customer's details     |
| Emp_Title             | StringType    | Title of employment                                |
| Emp_Length            | StringType    | Work experience of the customer                    |
| Home_Ownership        | StringType    | Type of home ownership                             |
| Annual_Income         | FloatType    | Annual income of the customer                      |
| Verification_Status   | StringType    | Verification status of customer's details          |
| Tot_Hi_Cred_Lim       | FloatType    | Maximum amount the borrower can request for        |
| Application_Type      | StringType    | Type of application (Individual or Joint)          |
| Annual_Income_Joint  | FloatType    | Combined annual income for joint applicants        |
| Verification_Status_Joint | StringType | Verification status for joint applicants           |

### Investor Table (investor):
  * Contains details of investors, such as Investor_Loan_Id, Loan_Id, Investor_Id, Funded_Amount_Inv, and more.

| Column Name        | Data Type | Description                                       |
|--------------------|-----------|---------------------------------------------------|
| Investor_Loan_Id   | IntegerType      | Investor ID for a given loan                      |
| Loan_id            | IntegerType      | Loan ID                                           |
| Investor_Id        | StringType    | Unique ID of the investor                         |
| Funded_amount_Inv  | FloatType    | Amount funded by the investor                     |
| Funded_Full        | BooleanType   | Whether full loan amount has been funded or not   |
| Investor_Type      | StringType    | Type of investor                                  |
| Investor_Age       | IntegerType      | Age of the investor                               |
| Investor_country   | StringType    | Country of the investor                           |
| Investor_state     | StringType    | State of the investor                             |

### Loan Defaulters Table (loan_defaulters):
  * Contains the history of loan defaulters, including Delinq_2yrs, Delinq_Amnt, Public_Records, and more.

| Column Name            | Data Type | Description                                           |
|------------------------|-----------|-------------------------------------------------------|
| Loan_Id                | IntegerType      | Loan ID                                               |
| Loan_Defaulter_Id      | StringType    | Defaulter Loan ID                                     |
| Delinq_2yrs            | IntegerType      | Number of delinquencies in the last two years        |
| Delinq_amnt            | FloatType    | Amount not paid by the borrower                      |
| Public_Records         | IntegerType      | Number of derogatory public records                  |
| Public_Record_Bankruptcies | IntegerType | Number of public record bankruptcies                 |
| Inq_last_6months       | IntegerType      | Credit inquiries in the last 6 months at the time of application   |
| Total_rec_late_fee     | FloatType    | Late fees received to date                           |
| Hardship_Flag          | BooleanType   | Flag indicating if the borrower has requested a hardship plan   |
| Hardship_Type          | StringType    | Type of hardship plan                                 |
| Hardship_Length        | StringType    | Length of the hardship plan                          |

### Payments Table (payments):
  * Contains payment-related information, including Total_Pymnt, Last_Pymnt_Id, Hardship_Amount, and more.

| Column Name       | Data Type | Description                                                                   |
|-------------------|-----------|-------------------------------------------------------------------------------|
| Loan_Id           | IntegerType      | Loan ID                                                                       |
| Transaction_Id    | StringType    | Transaction ID                                                                |
| Total_Pymnt       | FloatType    | Total payments received to date for the total amount funded                   |
| Last_Pymnt_Id     | StringType    | Last payment ID                                                               |
| Next_Pymnt_Id     | StringType    | Next payment ID                                                               |
| Hardship_Amount   | FloatType    | Interest payment that the borrower has committed to make each month while on a hardship plan   |
| Pymnt_Plan        | StringType    | Payment plan (Standard or Hardship)                                           |
| Last_pymnt_amnt   | FloatType    | Amount of the last payment received                                           |

## **Data Cleaning and Data Transformation**
* Data cleaning will be performed to handle missing values, duplicates, outliers, and standardize data.
* Data transformations will be carried out to calculate loan scores and perform other relevant transformations.

#### Data Transformation Specification 01 - LendingClub Customer Analytics
---
#### Scenario 1: Count of Total Customers, Grouped by State and Country

Step | Data Source             | Data Destination
---- | ----------------------- | ----------------------
1    | work.customer_details   | cooked.customers_total_count

#### Scenario 2: Count of Premium Customers, Grouped by State and Country

Step | Data Source             | Data Destination
---- | ----------------------- | ----------------------
1    | work.customer_details   | cooked.customers_premium_count

#### Scenario 3: Percentage of Premium Customers within Each State, Grouped by Country

Step | Data Source             | Data Destination
---- | ----------------------- | ------------------------------
1    | work.customer_details   | cooked.customers_premium_percentage

#### Scenario 4: Average Age of Customers, Grouped by State and Country

Step | Data Source             | Data Destination
---- | ----------------------- | ---------------------
1    | work.customer_details   | cooked.customers_avg_age

#### Additional Steps:

Step | Description
---- | ----------------------
1    | Create 'cooked' database if it doesn't exist.
2    | Execute Scala dataframe operations to create 'customersAvgAge' dataframe and save the results in 'cooked.customers_avg_age'.
3    | Execute the Spark SQL code to create temporary tables and perform transformations.

#### Data Transformation Specification 02 - LendingClub Loan Scoring
  --- 
#### Step 1: Set Spark Configuration Parameters

Setting Name | Value
------------ | -----
spark.sql.unacceptable_rated_pts | 0
spark.sql.very_bad_rated_pts | 100
spark.sql.bad_rated_pts | 250
spark.sql.good_rated_pts | 500
spark.sql.very_good_rated_pts | 650
spark.sql.excellent_rated_pts | 800
spark.sql.unacceptable_grade_pts | 750
spark.sql.very_bad_grade_pts | 1000
spark.sql.bad_grade_pts | 1500
spark.sql.good_grade_pts | 2000
spark.sql.very_good_grade_pts | 2500

#### Step 2: Load Data and Create Temporary Views

DataFrame          | Source Table
------------------ | ------------------
accountDf          | work.account_details
customerDf         | work.customer_details
loanDefaultersDf   | work.defaulter_details
loanDf             | work.loan_details
paymentDf          | work.payment_details
paymentLastDf      | paymentPointsDf
loanDefaulterPts   | loanDefaultPointsDf
financialDf        | loan_score_details
loanScore          | loan_score_pts
loanScoreFinal     | loan_score_eval
loan_score_final_grade | loan_final_table

#### Step 3: Execute Spark SQL Queries and Data Transformations

Step | Description
---- | -----------
1    | Calculate points for last payment and total payment of customers based on certain conditions.
2    | Calculate points for loan delinquency, public records, public bankruptcies, inquiries, and hardship status for defaulters.
3    | Calculate points for loan status, home ownership, credit limit, and grade for each customer.
4    | Calculate the final loan score based on payment history, defaulters history, and financial health points.
5    | Assign loan grades (A to F/G) based on the final loan score.

#### Step 4: Save Results

DataFrame             | Destination Table
--------------------- | ------------------
loan_score_final_grade | loan_final_table

#### Additional Steps:

Step | Description
---- | -----------
1    | Create an external table, cooked.customers_loan_score, using the Parquet format to store the final results.
2    | Display the records where loan_final_grade is NULL or 'A'.
3    | Exit the notebook with a success message.


## **Data Pipelines:**
* Data cleaning and transformation pipelines will be scheduled to run on specific intervals.
* Azure Key Vault will be integrated to securely store and access secrets required during the execution of pipelines.
## **Hive Tables and Views:**
* Hive Metastore will be used to store metadata information about Hive tables and partitions.
* Different types of views, such as Temp Views, Global Temp Views, and Permanent Views, will be created on top of tables for data analysis.

## Tech Stack:
 * Languages: Scala,SQL.
 * Services: Azure Data Factory, Azure Databricks, Azure Key Vault, Azure SQL Database, ADLS.
   
## Project Architecture:
![Final_Architecture_Diagram_Lending_Club](https://github.com/Madhusudhan985/LendingClub-Big-Data-Analytics/assets/62938921/68ee2a3e-518e-4a9e-8673-305cb0c5d641)

  
 The project involves several steps, including data ingestion, data cleaning, data transformation, and the creation of Hive tables and views for analysis. By implementing this project, the client, Lending Club, will gain valuable insights into customer data, improve loan assessment processes, and make more informed business decisions.


