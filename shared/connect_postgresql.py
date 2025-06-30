import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")

    database_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return psycopg2.connect(database_url)