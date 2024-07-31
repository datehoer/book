from sqlalchemy import Table, Column, Integer, String, DateTime
from MyDatabase import metadata, engine

import datetime

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String(50), unique=True, nullable=False),
    Column("email", String(100), unique=True, nullable=False),
    Column("hashed_password", String(100), nullable=False),
    Column("created_at", DateTime, default=datetime.datetime.utcnow),
)

metadata.create_all(engine)
