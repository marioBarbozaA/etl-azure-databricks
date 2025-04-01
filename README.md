# ETL Project with Azure Databricks, ADLS Gen2, Azure SQL, and Power BI

This README describes the complete **Extraction, Transformation, and Load (ETL)** process using **Azure Databricks**, **Azure Data Lake Storage Gen2**, **Azure Key Vault**, **Azure SQL Database**, and **Power BI**. The main goal is to analyze and present employee performance and productivity data from a **Kaggle dataset**:  
[Employee Performance and Productivity Data](https://www.kaggle.com/datasets/mexwell/employee-performance-and-productivity-data?resource=download)

---

## 1. Overview

- **Project Name**: `ETL_EmployeePerformance`  
- **Data Source**: [Employee Performance and Productivity Data on Kaggle](https://www.kaggle.com/datasets/mexwell/employee-performance-and-productivity-data?resource=download)  
- **Key Technologies**:
  - **Azure Data Lake Storage Gen2 (ADLS Gen2)** for raw and intermediate file storage.
  - **Azure Databricks** for data transformation in PySpark (following the Medallion Architecture).
  - **Azure Key Vault** to securely manage credentials and secrets.
  - **Azure SQL Database** for storing the Gold layer and facilitating direct consumption by apps or BI tools.
  - **Power BI** for interactive dashboards and final data visualizations.

The project follows the **Medallion Architecture** (Bronze → Silver → Gold) to ensure clean, standardized data and advanced business analytics.

---

## 2. Detailed ETL Flow

### 2.1. **Bronze Layer** (Raw Ingestion)

1. **Loading data into ADLS Gen2 (raw)**:
   - The original CSV file is uploaded to `abfss://<container>@<storage-account>.dfs.core.windows.net/raw/employee/...`.
2. **Reading from Databricks**:
   - A PySpark notebook reads the CSV into a DataFrame.
   - Basic validations (e.g., filtering out null `Employee_ID`).
3. **Storing Bronze**:
   - Data is stored in **Parquet or Delta** format under:  
     `abfss://<container>@<storage-account>.dfs.core.windows.net/bronze/employee`.

### 2.2. **Silver Layer** (Cleaning and Standardizing)

1. **Reading from Bronze**:
   - The DataFrame is read from the Bronze parquet/delta files.
2. **Standardization**:
   - Convert data types (e.g., turn `Hire_Date` into a true date),
   - Handle null or outlier values,
   - Possibly convert Boolean columns (`HasResigned`) to integers for numeric analysis.
3. **Storing Silver**:
   - The transformed data is saved to:  
     `abfss://<container>@<storage-account>.dfs.core.windows.net/silver/employee`.

### 2.3. **Gold Layer** (Business-Focused Transformations)

1. **Reading from Silver**:
   - Another notebook calculates KPIs and aggregations (by department, education level, etc.).
2. **Business Logic**:
   - Compute `Avg_Salary`, `Avg_Satisfaction`, `Resignation_Rate`, etc.
   - Possibly join or merge data for final analytics.
3. **Writing to Azure SQL Database**:
   - Connect via *Key Vault* + `ActiveDirectoryServicePrincipal` for secure authentication.
   - Final tables (Gold) are stored in SQL, e.g. `dbo.Gold_EducationSalary`, `dbo.Gold_DepartmentKPI`, `dbo.Gold_ResignationRateByAge`, etc.

---

## 3. Security with Azure Key Vault

- **Goal**: Avoid exposing secrets or access keys in code.
- **Implementation**:
  1. Create a **Secret Scope** in Databricks pointing to Azure Key Vault.
  2. Store your Storage Account key or SQL credentials (or Service Principal secrets) in Key Vault.
  3. Use `dbutils.secrets.get(...)` in your notebooks to retrieve them securely.

---

## 4. Presentation in Power BI

1. **Connecting to Azure SQL Database**:
   - Use server name `sqlsv-<project>.database.windows.net` and database name `sqldb-<project>`.
   - Use Azure AD credentials (not the service principal) to authenticate in Power BI.
2. **Importing Gold Tables**:
   - For example: `Gold_DepartmentKPI`, `Gold_ResignationRateByAge`, etc.
3. **Dashboard Design**:
   - **Slicers** (Dropdown) for filtering by `Department`, `Education_Level`, etc.
   - **Visuals**:
     - KPI cards (Employee Count, Avg Salary, etc.),
     - Column charts (comparing Salary vs. Satisfaction by Department),
     - Donut charts (distribution of Overtime Hours or Satisfaction),
     - Line or area charts (Age vs. Resignations).
   - A **unified color palette** (blues/greys) for a consistent look and feel.

---

## 5. Outcomes & Findings

- Salaries do not vary much by education level (in this synthetic dataset).
- Satisfaction might predict resignations, but further correlation analysis is needed.
- Departments with higher `Overtime_Hours` might show lower satisfaction (if the data indicates so).
- Extreme Ages may show higher or lower resignation rates, according to `Gold_ResignationRateByAge`.

---

## 6. Folder/File Structure

```
├─ notebooks
│   ├─ 01_Bronze_Ingestion.ipynb
│   ├─ 02_Silver_Transformation.ipynb
│   ├─ 03_Gold_AzureSQL.ipynb
├─ data
│   ├─ Extended_Employee_Performance_and_Productivity_Data.csv (or link to ADLS)
├─ docs
│   └─ diagrams, documentation
├─ power_bi
│   └─ EmployeeInsightsDashboard.pbix
└─ README.md  (this document)
```

---

## 7. Conclusion

This project demonstrates the **entire data lifecycle**:

1. **Ingestion** into Azure Data Lake (Bronze),  
2. **Cleaning** and standardizing in Silver,  
3. **Business analytics** in Gold,  
4. **Final load** into Azure SQL for consumption by **Power BI**.

We leveraged best practices in **security** (Key Vault), **scalability** (Databricks + ADLS), and **visualization** (an interactive dashboard with clear insights and slicers).

Future enhancements might include:
- **CI/CD** with Git & Azure DevOps,
- **Monitoring** costs with Databricks,
- **Machine Learning** expansions for churn prediction or advanced analytics.

---

### References

- [Kaggle Dataset: Employee Performance & Productivity](https://www.kaggle.com/datasets/mexwell/employee-performance-and-productivity-data?resource=download)  
- [Azure Databricks Documentation](https://docs.microsoft.com/azure/databricks/)  
- [Azure Data Lake Storage Gen2 Documentation](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)  
- [Azure SQL Database Docs](https://docs.microsoft.com/azure/azure-sql/)  
- [Power BI Desktop](https://powerbi.microsoft.com/desktop)
