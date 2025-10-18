#!/usr/bin/env python3
"""
Simple database initialization for the Temporal Order Workflow system.
"""
from database import db_manager

def init_database():
    """Initialize the database with all tables."""
    print("🗄️  Initializing database...")
    
    # Create engines and sessions
    db_manager.create_engine_and_session()
    db_manager.create_async_engine_and_session()
    
    # Create all tables
    db_manager.create_tables()
    
    print("Database initialized successfully!")

if __name__ == "__main__":
    init_database()