# 📊 Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀
This portfolio project demonstrates an end-to-end data warehousing and business intelligence solution. It showcases industry best practices in data engineering, from building a robust data architecture to generating actionable analytical insights.

---

## 🏗️ Data Architecture (Medallion Architecture)

The data flow in this project is built upon the **Medallion Architecture**, organizing data into three logical layers:

1. **🥉 Bronze Layer (Raw Data):** The landing zone where raw data from source systems (e.g., ERP and CRM CSV files) is ingested into the SQL Server Database without any transformations.
2. **🥈 Silver Layer (Cleansed Data):** The staging area where data undergoes cleansing, standardization, and normalization to resolve data quality issues and prepare it for analysis.
3. **🥇 Gold Layer (Curated Data):** The final layer containing business-ready data, modeled into a **Star Schema** (Fact and Dimension tables) optimized for high-performance analytical reporting.

---

## 📖 Project Overview

This repository is a comprehensive showcase of the following core data competencies:

* **Modern Data Architecture:** Designing a scalable data warehouse using the Bronze, Silver, and Gold layers.
* **ETL Pipelines:** Developing processes to Extract, Transform, and Load data seamlessly from source to destination.
* **Data Modeling:** Architecting optimized fact and dimension tables for complex querying.
* **Data Analytics & BI:** Writing advanced SQL-based reports and dashboards to uncover actionable business insights.

---

## 🛠️ Tech Stack & Tools

* **Database:** SQL Server (SQL Server Express)
* **Management Tool:** SQL Server Management Studio (SSMS)
* **Version Control:** Git & GitHub
* **Data Modeling & Architecture:** Draw.io
---

## 🚀 Project Requirements

### 1. Data Engineering (Building the Warehouse)
* **Data Sources:** Consolidate data from two distinct source systems (ERP and CRM) provided as CSV files.
* **Data Quality:** Identify and resolve formatting issues, duplicates, and inconsistencies before analysis.
* **Integration:** Merge both sources into a unified, user-friendly data model.
* **Scope:** Focus purely on current data states (historical data tracking is not required for this phase).

### 2. Data Analysis (Analytics & Reporting)
Develop SQL-based analytical queries to deliver insights into:
* **Customer Behavior:** Purchasing patterns and segment profitability.
* **Product Performance:** Top-selling categories and regional trends.
* **Sales Trends:** Revenue fluctuations over time.

---

## 📂 Repository Structure

```text
data-warehouse-project/
│
├── datasets/                   # Raw source datasets (ERP and CRM CSV files)
│
├── docs/                       # Architecture diagrams and documentation
│   ├── data_architecture.drawio
│   ├── data_models.drawio
│   ├── data_catalog.md         # Field descriptions and metadata
│   └── naming-conventions.md
│
├── scripts/                    # SQL scripts for ETL and data processing
│   ├── bronze/                 # DDL and ingestion scripts for raw data
│   ├── silver/                 # Transformation and data cleansing scripts
│   └── gold/                   # Star schema creation and analytical views
│
├── tests/                      # Data quality and validation checks
│
├── README.md                   # Main project documentation
└── .gitignore                  # Files ignored by Git
```
