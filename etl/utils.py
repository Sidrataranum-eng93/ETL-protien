# etl/utils.py
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.schema import init_db
from config import DB_URI, LOG_DIR

def setup_logging():
    """Set up logging configuration"""
    log_file = LOG_DIR / "etl.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def get_session():
    """Create and return a database session"""
    engine = init_db(DB_URI)
    Session = sessionmaker(bind=engine)
    return Session()