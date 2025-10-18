"""database config"""
import os
import asyncio
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from datetime import datetime
import asyncpg

Base = declarative_base()

class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(String, primary_key=True)
    state = Column(String, nullable=False)
    address_json = Column(JSONB)
    items = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    shipping_address = Column(JSONB)

class Item(Base):
    __tablename__ = 'items'
    sku = Column(String, primary_key=True)
    stock = Column(Integer, nullable=False)

class Payment(Base):
    __tablename__ = 'payments'
    
    payment_id = Column(String, primary_key=True)
    order_id = Column(String, nullable=False)
    status = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Event(Base):
    __tablename__ = 'events'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    payload_json = Column(JSONB)
    timestamp = Column(DateTime, default=datetime.utcnow)

class DatabaseManager:
    def __init__(self):
        self.database_url = self._get_database_url()
        self.async_database_url = self._get_async_database_url()
        self.engine = None
        self.async_engine = None
        self.SessionLocal = None
        self.AsyncSessionLocal = None
        
    def _get_database_url(self):
        """Get database URL from environment or use default for local development."""
        if os.environ.get('DOCKER_ENV') == 'true':
            # Running in Docker
            return "postgresql://temporal:temporal@postgresql:5432/temporal"
        else:
            # Local development
            return "postgresql://temporal:temporal@localhost:5432/temporal"
    
    def _get_async_database_url(self):
        """Get async database URL from environment or use default for local development."""
        if os.environ.get('DOCKER_ENV') == 'true':
            # Running in Docker
            return "postgresql+asyncpg://temporal:temporal@postgresql:5432/temporal"
        else:
            # Local development
            return "postgresql+asyncpg://temporal:temporal@localhost:5432/temporal"
    
    def create_engine_and_session(self):
        """Create SQLAlchemy engine and session factory."""
        self.engine = create_engine(self.database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        return self.engine, self.SessionLocal
    
    def create_async_engine_and_session(self):
        """Create async SQLAlchemy engine and session factory."""
        self.async_engine = create_async_engine(self.async_database_url)
        self.AsyncSessionLocal = async_sessionmaker(
            self.async_engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        return self.async_engine, self.AsyncSessionLocal
    
    def create_tables(self):
        """Create all tables."""
        if not self.engine:
            self.create_engine_and_session()
        Base.metadata.create_all(bind=self.engine)
        print("Database tables created successfully")
    
    def get_session(self):
        """Get a database session."""
        if not self.SessionLocal:
            self.create_engine_and_session()
        return self.SessionLocal()
    
    def get_async_session(self):
        """Get an async database session."""
        if not self.AsyncSessionLocal:
            self.create_async_engine_and_session()
        return self.AsyncSessionLocal()
    
    async def get_async_connection(self):
        """Get an async database connection for raw SQL operations."""
        if os.environ.get('DOCKER_ENV') == 'true':
            host = "postgresql"
        else:
            host = "localhost"
        
        return await asyncpg.connect(
            host=host,
            port=5432,
            user="temporal",
            password="temporal",
            database="temporal"
        )

# Global database manager instance
db_manager = DatabaseManager()
