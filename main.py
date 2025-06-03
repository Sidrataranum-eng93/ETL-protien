# main.py
import argparse
import logging
import time
from pathlib import Path

from etl.extract import extract_uniprot_data, extract_string_data, extract_opentargets_data
from etl.transform import transform_uniprot_to_proteins, transform_string_to_interactions, transform_opentargets_data
from etl.load import build_semantic_layer
from etl.utils import setup_logging, get_session
from config import DB_URI

def parse_args():
    parser = argparse.ArgumentParser(description='ETL pipeline for protein data integration')
    parser.add_argument('--stage', choices=['extract', 'transform', 'load', 'all'], default='all',
                        help='ETL stage to run (default: all)')
    parser.add_argument('--uniprot-only', action='store_true', 
                        help='Process only UniProt data')
    parser.add_argument('--string-only', action='store_true', 
                        help='Process only STRING data')
    parser.add_argument('--opentargets-only', action='store_true', 
                        help='Process only OpenTargets data')
    return parser.parse_args()

def main():
    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Parse command-line arguments
    args = parse_args()
    
    # Create database session
    session = get_session()
    
    try:
        # Extract stage
        if args.stage in ['extract', 'all']:
            logger.info("Starting data extraction")
            start_time = time.time()
            
            if not (args.string_only or args.opentargets_only):
                extract_uniprot_data(session)
            
            if not (args.uniprot_only or args.opentargets_only):
                extract_string_data(session)
            
            if not (args.uniprot_only or args.string_only):
                extract_opentargets_data(session)
            
            logger.info(f"Data extraction completed in {time.time() - start_time:.2f} seconds")
        
        # Transform stage
        if args.stage in ['transform', 'all']:
            logger.info("Starting data transformation")
            start_time = time.time()
            
            if not (args.string_only or args.opentargets_only):
                transform_uniprot_to_proteins(session)
            
            if not (args.uniprot_only or args.opentargets_only):
                transform_string_to_interactions(session)
            
            if not (args.uniprot_only or args.string_only):
                transform_opentargets_data(session)
            
            logger.info(f"Data transformation completed in {time.time() - start_time:.2f} seconds")
        
        # Load stage
        if args.stage in ['load', 'all']:
            logger.info("Starting semantic layer build")
            start_time = time.time()
            
            build_semantic_layer(session)
            
            logger.info(f"Semantic layer build completed in {time.time() - start_time:.2f} seconds")
        
        logger.info("ETL pipeline completed successfully")
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
    
    finally:
        session.close()

if __name__ == "__main__":
    main()