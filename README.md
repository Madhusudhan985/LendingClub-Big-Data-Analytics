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
*	Data Ingestion will be performed using Azure Data Lake Storage.
*	Azure Key Vault will be used to securely store credentials.

## Data Sources
### Customer Table (customer):
  * Contains basic details of borrowers, such as Cust_Id, Mem_Id, First_Name, Last_Name, Premium_Status, Age, State, and Country.

| Column Name | Data Type | Description                          |
|-------------|-----------|--------------------------------------|
| Cust_Id     | INT       | Customer ID                          |
| Mem_Id      | STRING    | Member ID from the Lending Club Platform |
| First_Name  | STRING    | First name of the customer           |
| Last_Name   | STRING    | Last name of the customer            |
| Prm_Status  | boolean   | Premium membership status            |
| Age         | INT       | Age of the customer                   |
| State       | STRING    | State where the customer resides     |
| Country     | STRING    | Country where the customer resides   |

### Loan Status Table (loan_status):
  * Contains details regarding loans, including Loan_Id, Mem_Id, Loan_Amount, Fund_Amount, Term, Interest Rate, Installment Amount, Issue Date, and Loan Status.

| Column Name       | Data Type | Description                                     |
|-------------------|-----------|-------------------------------------------------|
| Loan_Id           | INT       | Loan ID                                         |
| Mem_Id            | STRING    | Member ID from the Lending Club Platform        |
| Loan_amt          | FLOAT     | Requested loan amount                           |
| Fund_amt          | FLOAT     | Amount funded by investors                      |
| Term              | INT       | Repayment term in months                        |
| Interest_Rate     | FLOAT     | Interest rate on the loan                       |
| Installment_Amount| FLOAT     | Installment amount to be paid                   |
| Issue_Date        | DATE      | Date when the loan was issued                   |
| Loan_Status       | STRING    | Status of the loan (Current, Charged Off, Fully Paid, etc.) |
| Purpose           | STRING    | Category provided by the borrower for the loan request |
| Title             | STRING    | Title provided by the borrower for the loan request |
| Disbursement_Method | STRING  | Mode of payment for the loan (Cash, Direct Pay, etc.) |

### Account Table (account):
 * Contains borrower account details, including Acc_Id, Mem_Id, Grade, Sub_Grade, Emp_Title, Emp_Length, Home_Ownership, Annual_Income, and more.

| Column Name           | Data Type | Description                                        |
|-----------------------|-----------|----------------------------------------------------|
| Acc_Id                | INT       | Account ID                                         |
| Mem_Id                | STRING    | Member ID from the Lending Club Platform           |
| Grade                 | char      | Grade assigned based on customer's details         |
| Sub_Grade             | char      | Sub-grade assigned based on customer's details     |
| Emp_Title             | STRING    | Title of employment                                |
| Emp_Length            | STRING    | Work experience of the customer                    |
| Home_Ownership        | STRING    | Type of home ownership                             |
| Annual_Income         | FLOAT     | Annual income of the customer                      |
| Verification_Status   | STRING    | Verification status of customer's details          |
| Tot_Hi_Cred_Lim       | FLOAT     | Maximum amount the borrower can request for        |
| Application_Type      | STRING    | Type of application (Individual or Joint)          |
| Annual_Income_Joint   | FLOAT     | Combined annual income for joint applicants        |
| Verification_Status_Joint | STRING | Verification status for joint applicants           |

### Investor Table (investor):
  * Contains details of investors, such as Investor_Loan_Id, Loan_Id, Investor_Id, Funded_Amount_Inv, and more.

| Column Name        | Data Type | Description                                       |
|--------------------|-----------|---------------------------------------------------|
| Investor_Loan_Id   | INT       | Investor ID for a given loan                      |
| Loan_id            | INT       | Loan ID                                           |
| Investor_Id        | STRING    | Unique ID of the investor                         |
| Funded_amount_Inv  | FLOAT     | Amount funded by the investor                     |
| Funded_Full        | boolean   | Whether full loan amount has been funded or not   |
| Investor_Type      | STRING    | Type of investor                                  |
| Investor_Age       | INT       | Age of the investor                               |
| Investor_country   | STRING    | Country of the investor                           |
| Investor_state     | STRING    | State of the investor                             |

### Loan Defaulters Table (loan_defaulters):
  * Contains the history of loan defaulters, including Delinq_2yrs, Delinq_Amnt, Public_Records, and more.

| Column Name            | Data Type | Description                                           |
|------------------------|-----------|-------------------------------------------------------|
| Loan_Id                | INT       | Loan ID                                               |
| Loan_Defaulter_Id      | STRING    | Defaulter Loan ID                                     |
| Delinq_2yrs            | INT       | Number of delinquencies in the last two years        |
| Delinq_amnt            | FLOAT     | Amount not paid by the borrower                      |
| Public_Records         | INT       | Number of derogatory public records                  |
| Public_Record_Bankruptcies | INT  | Number of public record bankruptcies                 |
| Inq_last_6months       | INT       | Credit inquiries in the last 6 months at the time of application   |
| Total_rec_late_fee     | FLOAT     | Late fees received to date                           |
| Hardship_Flag          | boolean   | Flag indicating if the borrower has requested a hardship plan   |
| Hardship_Type          | STRING    | Type of hardship plan                                 |
| Hardship_Length        | STRING    | Length of the hardship plan                          |

### Payments Table (payments):
  * Contains payment-related information, including Total_Pymnt, Last_Pymnt_Id, Hardship_Amount, and more.

| Column Name       | Data Type | Description                                                                   |
|-------------------|-----------|-------------------------------------------------------------------------------|
| Loan_Id           | INT       | Loan ID                                                                       |
| Transaction_Id    | STRING    | Transaction ID                                                                |
| Total_Pymnt       | FLOAT     | Total payments received to date for the total amount funded                   |
| Last_Pymnt_Id     | STRING    | Last payment ID                                                               |
| Next_Pymnt_Id     | STRING    | Next payment ID                                                               |
| Hardship_Amount   | FLOAT     | Interest payment that the borrower has committed to make each month while on a hardship plan   |
| Pymnt_Plan        | STRING    | Payment plan (Standard or Hardship)                                           |
| Last_pymnt_amnt   | FLOAT     | Amount of the last payment received                                           |



