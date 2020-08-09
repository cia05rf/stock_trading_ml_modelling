"""Model for sql database"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from pathlib import Path

from config import CONFIG

#Start the engine and Session
engine = create_engine(
    f'sqlite:///{str(Path(CONFIG["files"]["store_path"]) / CONFIG["files"]["prices_db"])}'
)
Session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

Base = declarative_base()