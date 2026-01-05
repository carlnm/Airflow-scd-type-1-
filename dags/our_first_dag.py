from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import psycopg2
from psycopg2.extras import execute_values

# Connection Configuration

MSSQL_CONFIG = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "host.docker.internal\\SQLEXPRESS",
    "database": "CARL",
    "username": "sa",
    "password": "123",
    "port": 1433
}

POSTGRES_CONFIG = {
    "host": "host.docker.internal",
    "port": 5432,
    "dbname": "stg",
    "user": "postgres",
    "password": "123"
}

def get_mssql_conn():
    conn_str = (
        f"DRIVER={{{MSSQL_CONFIG['driver']}}};"
        f"SERVER={MSSQL_CONFIG['server']};"
        f"DATABASE={MSSQL_CONFIG['database']};"
        f"UID={MSSQL_CONFIG['username']};"
        f"PWD={MSSQL_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str, timeout=30)


def get_pg_conn():
    return psycopg2.connect(**POSTGRES_CONFIG)

# ETL Process
def extract_product():
    mssql = get_mssql_conn()
    pg = get_pg_conn()
    try:
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
            cur,"""
            INSERT INTO stg.product_raw 
            (productid, productname, productnumber, subcategoryname, categoryname, listprice) 
            VALUES %s
            """,
            df[['ProductID','productname','ProductNumber','subcategoryname','categoryname','ListPrice']].values.tolist()
        )
        pg.commit()
        cur.close()

    finally:
        mssql.close()
        pg.close()

def extract_customer():
    mssql = get_mssql_conn()
    pg = get_pg_conn()
    try:
        sql = """
         SELECT
            c.CustomerID,
            COALESCE(p.FirstName,'UNKNOWN') AS firstname,
            COALESCE(p.LastName,'UNKNOWN') AS lastname,
            COALESCE(c.TerritoryID,0) AS territoryid
            
        FROM
            sales.Customer as c
            JOIN Person.Person as p ON c.PersonID = p.BusinessEntityID
        """
        df = pd.read_sql(sql, mssql)
        cur = pg.cursor()
        cur.execute("TRUNCATE TABLE stg.customer_raw")
        execute_values(
            cur, """
                    INSERT INTO stg.customer_raw
                    (customerid, firstname, lastname, territoryid) 
                    VALUES %s
                    """,
            df[['CustomerID','firstname','lastname','territoryid']].values.tolist()
        )
        pg.commit()
        cur.close()
    finally:
        mssql.close()
        pg.close()

def extract_territory():
    mssql = get_mssql_conn()
    pg = get_pg_conn()
    try:
        sql = """
            SELECT 
                TerritoryID as territoryid,
                COALESCE(Name,'UNKNOWN') AS territoryname,
                COALESCE(CountryRegionCode,'UNKNOWN') AS countrycode
            FROM
                Sales.SalesTerritory
           """
        df = pd.read_sql(sql, mssql)
        cur = pg.cursor()
        cur.execute("TRUNCATE TABLE stg.territory_raw")
        execute_values(
            cur, """
                       INSERT INTO stg.territory_raw 
                       (territoryid, territoryname, countrycode)
                       VALUES %s
                       """,
            df[['territoryid', 'territoryname', 'countrycode']].values.tolist()
        )
        pg.commit()
        cur.close()
    finally:
        mssql.close()
        pg.close()

def extract_sales():
    mssql = get_mssql_conn()
    pg = get_pg_conn()
    try:
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
            FROM
                Sales.SalesOrderHeader as SOH 
                JOIN Sales.SalesOrderDetail as sod ON soh.SalesOrderID = sod.SalesOrderID
             """
        df = pd.read_sql(sql, mssql)
        cur = pg.cursor()
        cur.execute("TRUNCATE TABLE stg.sales_raw")
        execute_values(
            cur, """
                         INSERT INTO stg.sales_raw
                         (salesorderid, salesorderdetailid, orderdate, customerid, territoryid, productid, orderqty, unitprice, linetotal)
                         VALUES %s
                         """,
            df[['SalesOrderID','SalesOrderDetailID','OrderDate','CustomerID','TerritoryID','ProductID','OrderQty','UnitPrice','LineTotal']].values.tolist()
        )
        pg.commit()
        cur.close()
    finally:
        mssql.close()
        pg.close()

def extract_date():
    mssql = get_mssql_conn()
    pg = get_pg_conn()
    try:
        sql = """
            SELECT DISTINCT
                CAST(OrderDate AS DATE) AS full_date,
                DAY(OrderDate) AS day_number_of_month,
                DATENAME(MONTH, OrderDate) AS month_name,
                MONTH(OrderDate) AS month_number_of_year,
                YEAR(OrderDate) AS calendar_year
            FROM Sales.SalesOrderHeader
            WHERE OrderDate IS NOT NULL
            ORDER BY full_date
        """
        # Load data from MSSQL
        df = pd.read_sql(sql, mssql)

        # Load into staging table in PostgreSQL
        cur = pg.cursor()
        try:
            cur.execute("TRUNCATE TABLE stg.date_raw")
            execute_values(
                cur,
                """
                INSERT INTO stg.date_raw
                    (full_date, day_number_of_month, month_name, month_number_of_year, calendar_year)
                VALUES %s
                """,
                df[['full_date', 'day_number_of_month', 'month_name', 'month_number_of_year', 'calendar_year']].values.tolist()
            )
            pg.commit()
        finally:
            cur.close()
    finally:
        mssql.close()
        pg.close()



#Load Dimensions

def load_dim_product():
    pg = get_pg_conn()
    try:
        cur = pg.cursor()
        try:
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
        finally:cur.close()
    finally:
        pg.close()

def load_dim_customer():
    pg = get_pg_conn()
    try:
        cur = pg.cursor()
        try:
            cur.execute("""
                        INSERT INTO public.dimcustomer (customerid, fullname, territoryid)
                        SELECT
                            COALESCE(NULLIF(customerid::numeric, 'NaN')::integer, 0) AS customerid,
                            CONCAT(firstname, ' ', lastname) AS fullname,
                            COALESCE(NULLIF(territoryid::numeric, 'NaN')::integer, 0) AS territoryid
                        FROM stg.customer_raw
                            ON CONFLICT (customerid) DO UPDATE
                            SET fullname = EXCLUDED.fullname,
                            territoryid = EXCLUDED.territoryid;

                        """)
            pg.commit()
        finally:cur.close()
    finally:
        pg.close()

def load_dim_territory():
    pg = get_pg_conn()
    try:
        cur = pg.cursor()
        try:
            cur.execute("""
                       INSERT INTO public.dimsalesterritory (territoryid, territoryname, countryregioncode)
                        SELECT 
                            territoryid, 
                            territoryname, 
                            countrycode
                        FROM stg.territory_raw
                            ON CONFLICT (territoryid) DO UPDATE SET
                            territoryname = EXCLUDED.territoryname,
                            countryregioncode = EXCLUDED.countryregioncode

                        """)
            pg.commit()
        finally:cur.close()
    finally:
        pg.close()

def load_dim_date():
    pg = get_pg_conn()
    try:
        cur = pg.cursor()
        try:
            cur.execute("""
                INSERT INTO public.dimdate (
                    datekey,
                    full_date,
                    day_number_of_month,
                    month_name,
                    month_number_of_year,
                    calendar_year
                )
                SELECT
                    TO_CHAR(full_date, 'YYYYMMDD')::INT AS datekey,
                    full_date,
                    day_number_of_month,
                    month_name,
                    month_number_of_year,
                    calendar_year
                FROM stg.date_raw
                ON CONFLICT (datekey) DO NOTHING
            """)
            pg.commit()
        finally:
            cur.close()
    finally:
        pg.close()



def load_fact_sales():
    pg = get_pg_conn()
    try:
        cur = pg.cursor()
        try:
            cur.execute("DELETE FROM public.factsales")

            cur.execute("""
                INSERT INTO public.factsales (
                    datekey,
                    productkey,
                    customerkey,
                    territorykey,
                    salesorderid,      
                    salesorderdetailid,
                    orderqty,
                    unitprice,
                    linetotal
                )
                SELECT
                    dd.datekey,
                    dp.productkey,
                    dc.customerkey,
                    dt.territorykey,
                    s.salesorderid,       
                    s.salesorderdetailid,
                    s.orderqty,
                    s.unitprice::numeric,
                    s.linetotal::numeric
                FROM stg.sales_raw s
                JOIN public.dimdate dd
                    ON s.orderdate::date = dd.full_date
                JOIN public.dimproduct dp
                    ON s.productid = dp.productid
                JOIN public.dimcustomer dc
                    ON s.customerid = dc.customerid
                JOIN public.dimsalesterritory dt
                    ON s.territoryid = dt.territoryid
            """)

            pg.commit()

        except Exception as e:
            pg.rollback()
            print("FACT SALES LOAD FAILED:", e)
            raise
        finally:
            cur.close()
    finally:
        pg.close()


#Dag

default_args={
    "owner": "Carl",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="adventureworks_full_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    task1 = PythonOperator(
        task_id="extract_product",
        python_callable=extract_product
    )
    task2 = PythonOperator(
        task_id="extract_customer",
        python_callable=extract_customer
    )
    task3 = PythonOperator(
        task_id="extract_territory",
        python_callable=extract_territory
    )
    task4 = PythonOperator(
        task_id="extract_date",
        python_callable=extract_date
    )
    task5 = PythonOperator(
        task_id="extract_sales_fact",
        python_callable=extract_sales
    )



    dim1 = PythonOperator(
        task_id="load_dim_product",
        python_callable=load_dim_product
    )
    dim2 = PythonOperator(
        task_id="load_dim_customer",
        python_callable=load_dim_customer
    )
    dim3 = PythonOperator(
        task_id="load_dim_territory",
        python_callable=load_dim_territory
    )
    dim4 = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date
    )

    fact1 = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales
    )


t_tasks = [task1, task2, task3, task4, task5]
d_tasks = [dim1, dim2, dim3, dim4]

for t in t_tasks:
    for d in d_tasks:
        t >> d

for d in d_tasks:
    d >> fact1


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












