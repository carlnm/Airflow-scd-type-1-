import psycopg2
conn = psycopg2.connect(
    host="host.docker.internal",
    port=5432,
    dbname="stg",
    user="postgres",
    password="123"
)
print("Connected successfully!")
