import os
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    
    id = Column(String, primary_key=True)
    username = Column(String, unique=True, index=True)
    display_name = Column(String)
    bio = Column(String)
    follower_count = Column(Integer)
    following_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    videos = relationship("Video", back_populates="user")

class Video(Base):
    __tablename__ = "videos"
    __table_args__ = {"schema": "public"}
    
    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"))
    caption = Column(String)
    create_time = Column(DateTime)
    like_count = Column(Integer)
    comment_count = Column(Integer)
    view_count = Column(Integer)
    share_count = Column(Integer)
    collected_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="videos")

class VideoMetricsHourly(Base):
    __tablename__ = "video_metrics_hourly"
    __table_args__ = {"schema": "public"}
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String, ForeignKey("videos.id"))
    hour = Column(DateTime)
    like_count = Column(Integer)
    comment_count = Column(Integer)
    view_count = Column(Integer)
    share_count = Column(Integer)
    collected_at = Column(DateTime, default=datetime.utcnow)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close() 