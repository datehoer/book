from sqlalchemy import create_engine, MetaData
from databases import Database
from config import MYSQL_CONFIG
DATABASE_URL = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}"

database = Database(DATABASE_URL)
metadata = MetaData()

engine = create_engine(
    DATABASE_URL
)
