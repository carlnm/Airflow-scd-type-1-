from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import psycopg2
from psycopg2.extras import execute_values

# =========================================================
# HARD-CODED CONNECTION CONFIGS
# =========================================================

MSSQL_CONFIG = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "host.docker.internal\\SQLEXPRESS",
    "database": "CARL",
    "username": "sa",
    "password": "123",
    "port": 1433
}

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "stg",
    "user": "postgres",
    "password": "123"
}

# =========================================================
# CONNECTION HELPERS
# =========================================================

def get_mssql_conn():
    conn_str = (
        f"DRIVER={{{MSSQL_CONFIG['driver']}}};"
        f"SERVER={MSSQL_CONFIG['server']};"
        f"DATABASE={MSSQL_CONFIG['database']};"
        f"UID={MSSQL_CONFIG['username']};"
        f"PWD={MSSQL_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)

def get_pg_conn():
    return psycopg2.connect(**POSTGRES_CONFIG)

# =========================================================
# ETL FUNCTIONS
# =========================================================

# --- Extract ---
def extract_product():
    print("Extracting product...")
    mssql = get_mssql_conn()
    pg = get_pg_conn()

    sql = """
        SELECT
	p.ProductID,
	p.Name as productname,
	p.ProductNumber,
	COALESCE (ps.name,'UNKNOWN') as subcategoryname,
	COALESCE (pc.Name,'UNKNOWN')as categoryname,
	p.ListPrice
    FROM
        Production.Product AS p
        LEFT JOIN Production.ProductSubcategory as ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID 
        LEFT JOIN Production.ProductCategory as pc ON ps.ProductCategoryID = pc.ProductCategoryID
    """
    df = pd.read_sql(sql, mssql)

    cur = pg.cursor()
    cur.execute("TRUNCATE TABLE stg.product_raw")
    execute_values(
        cur,
        "INSERT INTO stg.product_raw (productid, productname, productnumber, subcategoryname, categoryname, listprice) VALUES %s",
        df[['ProductID','productname','ProductNumber','subcategoryname','categoryname','ListPrice']].values.tolist()
    )
    pg.commit()
    cur.close()
    mssql.close()
    pg.close()
    print("Product extracted and staged.")

def extract_customer():
    print("Extracting customer...")
    mssql = get_mssql_conn()
    pg = get_pg_conn()

    sql = """
        SELECT
            c.CustomerID,
            p.FirstName,
            p.LastName,
            c.TerritoryID
        FROM Sales.Customer c
        JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    """
    df = pd.read_sql(sql, mssql)

    cur = pg.cursor()
    cur.execute("TRUNCATE TABLE stg.customer_raw")
    execute_values(
        cur,
        "INSERT INTO stg.customer_raw (customerid, firstname, lastname, territoryid) VALUES %s",
        df[['CustomerID','FirstName','LastName','TerritoryID']].values.tolist()
    )
    pg.commit()
    cur.close()
    mssql.close()
    pg.close()
    print("Customer extracted and staged.")

def extract_territory():
    print("Extracting territory...")
    mssql = get_mssql_conn()
    pg = get_pg_conn()

    sql = "SELECT TerritoryID, Name AS territoryname, CountryRegionCode AS countrycode FROM Sales.SalesTerritory"
    df = pd.read_sql(sql, mssql)

    cur = pg.cursor()
    cur.execute("TRUNCATE TABLE stg.territory_raw")
    execute_values(
        cur,
        "INSERT INTO stg.territory_raw (territoryid, territoryname, countrycode) VALUES %s",
        df[['TerritoryID','territoryname','countrycode']].values.tolist()
    )
    pg.commit()
    cur.close()
    mssql.close()
    pg.close()
    print("Territory extracted and staged.")

def extract_sales():
    print("Extracting sales...")
    mssql = get_mssql_conn()
    pg = get_pg_conn()

    sql = """
        SELECT
            soh.SalesOrderID,
            sod.SalesOrderDetailID,
            soh.OrderDate,
            soh.CustomerID,
            soh.TerritoryID,
            sod.ProductID,
            sod.OrderQty,
            sod.UnitPrice,
            sod.LineTotal
        FROM Sales.SalesOrderHeader soh
        JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    """
    df = pd.read_sql(sql, mssql)

    cur = pg.cursor()
    cur.execute("TRUNCATE TABLE stg.sales_raw")
    execute_values(
        cur,
        "INSERT INTO stg.sales_raw (salesorderid, salesorderdetailid, orderdate, customerid, territoryid, productid, orderqty, unitprice, linetotal) VALUES %s",
        df[['SalesOrderID','SalesOrderDetailID','OrderDate','CustomerID','TerritoryID','ProductID','OrderQty','UnitPrice','LineTotal']].values.tolist()
    )
    pg.commit()
    cur.close()
    mssql.close()
    pg.close()
    print("Sales extracted and staged.")

# --- Load Dimensions ---
def load_dim_product():
    print("Loading dim_product...")
    pg = get_pg_conn()
    cur = pg.cursor()
    cur.execute("""
        INSERT INTO public.dimproduct (productid, productname, productnumber, listprice)
        SELECT productid, productname, productnumber, listprice
        FROM stg.product_raw
        ON CONFLICT (productid) DO UPDATE SET
            productname = EXCLUDED.productname,
            productnumber = EXCLUDED.productnumber,
            listprice = EXCLUDED.listprice
    """)
    pg.commit()
    cur.close()

    pg.close()
    print("dim_product loaded.")

def load_dim_customer():
    print("Loading dim_customer...")
    pg = get_pg_conn()
    cur = pg.cursor()
    cur.execute("""
        INSERT INTO public.dimcustomer (customerid, fullname, territoryid)
SELECT
    COALESCE(NULLIF(customerid::numeric, 'NaN')::integer, 0) AS customerid,
    firstname || ' ' || lastname AS fullname,
    COALESCE(NULLIF(territoryid::numeric, 'NaN')::integer, 0) AS territoryid
FROM stg.customer_raw
ON CONFLICT (customerid) DO UPDATE
SET fullname = EXCLUDED.fullname,
    territoryid = EXCLUDED.territoryid;

    """)
    pg.commit()
    cur.close()
    pg.close()
    print("dim_customer loaded.")



def load_dim_territory():
    print("Loading dim_territory...")
    pg = get_pg_conn()
    cur = pg.cursor()
    cur.execute("""
        INSERT INTO public.dimsalesterritory (territoryid, territoryname, countryregioncode)
        SELECT territoryid, territoryname, countrycode
        FROM stg.territory_raw
        ON CONFLICT (territoryid) DO UPDATE SET
            territoryname = EXCLUDED.territoryname,
            countryregioncode = EXCLUDED.countryregioncode
    """)
    pg.commit()
    cur.close()
    pg.close()
    print("dim_territory loaded.")

# --- Load Fact ---
def load_fact_sales():
    print("Loading fact_sales...")
    pg = get_pg_conn()
    cur = pg.cursor()
    cur.execute("TRUNCATE TABLE public.factsales")
    cur.execute("""
        INSERT INTO public.factsales (datekey, productkey, customerkey, territorykey, orderqty, unitprice, linetotal)
        SELECT
            dd.datekey,
            dp.productkey,
            dc.customerkey,
            dt.territorykey,
            s.orderqty,
            s.unitprice,
            s.linetotal
        FROM stg.sales_raw s
        JOIN public.dimdate dd ON s.orderdate::date = dd.fulldate
        JOIN public.dimproduct dp ON s.productid = dp.productid
        JOIN public.dimcustomer dc ON s.customerid = dc.customerid
        JOIN public.dimsalesterritory dt ON s.territoryid = dt.territoryid
    """)
    pg.commit()
    cur.close()
    pg.close()
    print("fact_sales loaded.")

# =========================================================
# DAG DEFINITION FOR AIRFLOW
# =========================================================

default_args = {
    "owner": "Carl",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="adventureworks_full_load",
    start_date=datetime(2025, 12, 31),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    # Extract Tasks
    t1 = PythonOperator(task_id="extract_product", python_callable=extract_product)
    t2 = PythonOperator(task_id="extract_customer", python_callable=extract_customer)
    t3 = PythonOperator(task_id="extract_territory", python_callable=extract_territory)
    t4 = PythonOperator(task_id="extract_sales_fact", python_callable=extract_sales)

    # Dimension Tasks
    d1 = PythonOperator(task_id="load_dim_product", python_callable=load_dim_product)
    d2 = PythonOperator(task_id="load_dim_customer", python_callable=load_dim_customer)
    d3 = PythonOperator(task_id="load_dim_territory", python_callable=load_dim_territory)

    # Fact Task
    f1 = PythonOperator(task_id="load_fact_sales", python_callable=load_fact_sales)

    # Set dependencies
    t_tasks = [t1, t2, t3, t4]
    d_tasks = [d1, d2, d3]

    for t in t_tasks:
        for d in d_tasks:
            t >> d

    for d in d_tasks:
        d >> f1

# =========================================================
# DIRECT PYTHON EXECUTION FOR TESTING
if __name__ == "__main__":
    extract_product()
    extract_customer()
    extract_territory()
    extract_sales()
    load_dim_product()
    load_dim_customer()
    load_dim_territory()
    load_fact_sales()
    print("ETL completed successfully!")
