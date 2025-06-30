# shared/get_sqlalchemy_engine.py
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def get_sqlalchemy_engine():
    load_dotenv()
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
