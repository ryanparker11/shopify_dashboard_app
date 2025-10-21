import os

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")  # 'require' for RDS/Aurora

# psycopg DSN
DATABASE_DSN = (
    f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} sslmode={DB_SSLMODE}"
)
