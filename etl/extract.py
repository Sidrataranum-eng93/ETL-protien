# etl/extract.py
import gzip
import logging
import pandas as pd
import pyarrow.parquet as pq
from lxml import etree
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from models.schema import (
    StagingUniProt, StagingString, StagingOpenTargetsTarget,
    StagingOpenTargetsDisease, StagingOpenTargetsAssociation
)
from config import (
    UNIPROT_XML_PATH, STRING_DATA_PATH, OPENTARGETS_TARGETS_PATH,
    OPENTARGETS_DISEASES_PATH, OPENTARGETS_ASSOCIATIONS_PATH, BATCH_SIZE
)

logger = logging.getLogger(__name__)

def extract_uniprot_data(session, xml_path=UNIPROT_XML_PATH):
    """
    Extract data from UniProt XML file and load to staging table
    
    Args:
        session: SQLAlchemy session
        xml_path: Path to UniProt XML file
    """
    logger.info(f"Extracting UniProt data from {xml_path}")
    
    # Define namespaces
    namespaces = {
        'uniprot': 'https://uniprot.org/uniprot',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }
    
    # Functions to extract data from each entry
    def get_accession(entry):
        return entry.xpath('./uniprot:accession[1]/text()', namespaces=namespaces)[0]
    
    def get_protein_name(entry):
        names = entry.xpath('./uniprot:protein/uniprot:recommendedName/uniprot:fullName/text()', namespaces=namespaces)
        if names:
            return names[0]
        return None
    
    def get_gene_name(entry):
        # Try primary gene name first
        genes = entry.xpath('./uniprot:gene/uniprot:name[@type="primary"]/text()', namespaces=namespaces)
        if genes:
            return genes[0]
        
        # If no primary name, try ordered locus name
        genes = entry.xpath('./uniprot:gene/uniprot:name[@type="ordered locus"]/text()', namespaces=namespaces)
        if genes:
            return genes[0]
            
        # If still no name, try any gene name
        genes = entry.xpath('./uniprot:gene/uniprot:name/text()', namespaces=namespaces)
        if genes:
            return genes[0]
            
        return None
    
    def get_species(entry):
        # Try common name first
        species = entry.xpath('./uniprot:organism/uniprot:name[@type="common"]/text()', namespaces=namespaces)
        if species:
            return species[0]
        
        # If no common name, use scientific name
        species = entry.xpath('./uniprot:organism/uniprot:name[@type="scientific"]/text()', namespaces=namespaces)
        if species:
            return species[0]
            
        return None
    
    def get_string_id(entry):
        refs = entry.xpath('./uniprot:dbReference[@type="STRING"]/@id', namespaces=namespaces)
        if refs:
            return refs[0]
        return None
    
    def get_opentargets_id(entry):
        refs = entry.xpath('./uniprot:dbReference[@type="OpenTargets"]/@id', namespaces=namespaces)
        if refs:
            return refs[0]
        return None
    
    def get_sequence_length(entry):
        seq = entry.xpath('./uniprot:sequence', namespaces=namespaces)
        if seq and 'length' in seq[0].attrib:
            return int(seq[0].attrib['length'])
        return None
    
    def get_sequence_mass(entry):
        seq = entry.xpath('./uniprot:sequence', namespaces=namespaces)
        if seq and 'mass' in seq[0].attrib:
            return int(seq[0].attrib['mass'])
        return None
    
    def get_sequence(entry):
        seq = entry.xpath('./uniprot:sequence/text()', namespaces=namespaces)
        if seq:
            return seq[0].strip()
        return None
    
    # Process XML file using iterparse to avoid loading the entire file into memory
    batch = []
    
    # Check if file is gzipped
    open_func = gzip.open if str(xml_path).endswith('.gz') else open
    
    with open_func(xml_path, 'rb') as f:
        # Use iterparse to process the file in a memory-efficient way
        context = etree.iterparse(f, events=('end',), tag=f'{{{namespaces["uniprot"]}}}entry')
        
        for event, elem in tqdm(context, desc="Processing UniProt entries"):
            try:
                # Extract data
                accession = get_accession(elem)
                protein_record = StagingUniProt(
                    accession=accession,
                    protein_name=get_protein_name(elem),
                    gene_name=get_gene_name(elem),
                    species=get_species(elem),
                    string_id=get_string_id(elem),
                    opentargets_id=get_opentargets_id(elem),
                    sequence_length=get_sequence_length(elem),
                    sequence_mass=get_sequence_mass(elem),
                    sequence=get_sequence(elem)
                )
                
                batch.append(protein_record)
                
                # Process in batches to conserve memory
                if len(batch) >= BATCH_SIZE:
                    session.add_all(batch)
                    session.commit()
                    batch = []
                
                # Clear memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
            
            except Exception as e:
                logger.error(f"Error processing UniProt entry (accession may be {get_accession(elem) if 'get_accession' in locals() else 'unknown'}): {e}")
                # Continue processing other entries
                continue
        
        # Add any remaining records
        if batch:
            try:
                session.add_all(batch)
                session.commit()
            except Exception as e:
                logger.error(f"Error committing final batch: {e}")
                session.rollback()
    
    # Get count of loaded records
    try:
        count = session.query(StagingUniProt).count()
        logger.info(f"Loaded {count} UniProt records to staging")
    except Exception as e:
        logger.error(f"Error counting records: {e}")

def extract_string_data(session, file_path=STRING_DATA_PATH):
    """
    Extract data from STRING file and load to staging table
    
    Args:
        session: SQLAlchemy session
        file_path: Path to STRING data file
    """
    logger.info(f"Extracting STRING data from {file_path}")
    
    # Process in chunks to conserve memory
    chunksize = BATCH_SIZE
    
    # Check if file is gzipped
    open_func = gzip.open if str(file_path).endswith('.gz') else open
    
    # Get total number of lines for progress reporting
    with open_func(file_path, 'rt') as f:
        total_lines = sum(1 for _ in f)
    
    # Process file
    processed_lines = 0
    
    with open_func(file_path, 'rt') as f:
        # Skip header
        header = f.readline()
        processed_lines += 1
        
        batch = []
        
        for line in tqdm(f, total=total_lines-1, desc="Processing STRING interactions"):
            try:
                protein1, protein2, combined_score = line.strip().split()[:3]
                
                # Convert combined_score to integer
                combined_score = int(combined_score)
                
                interaction = StagingString(
                    protein1=protein1,
                    protein2=protein2,
                    combined_score=combined_score
                )
                
                batch.append(interaction)
                
                # Process in batches
                if len(batch) >= chunksize:
                    session.add_all(batch)
                    session.commit()
                    batch = []
            
            except Exception as e:
                logger.error(f"Error processing STRING line: {e}")
                session.rollback()
            
            processed_lines += 1
        
        # Add any remaining records
        if batch:
            session.add_all(batch)
            session.commit()
    
    # Get count of loaded records
    count = session.query(StagingString).count()
    logger.info(f"Loaded {count} STRING records to staging")

def extract_opentargets_data(session, targets_path=OPENTARGETS_TARGETS_PATH, 
                            diseases_path=OPENTARGETS_DISEASES_PATH,
                            associations_path=OPENTARGETS_ASSOCIATIONS_PATH):
    """
    Extract data from OpenTargets parquet files and load to staging tables
    
    Args:
        session: SQLAlchemy session
        targets_path: Path to OpenTargets targets parquet files
        diseases_path: Path to OpenTargets diseases parquet files
        associations_path: Path to OpenTargets associations parquet files
    """
    # Extract targets data
    logger.info(f"Extracting OpenTargets targets data from {targets_path}")
    targets_path = Path(targets_path)
    
    if targets_path.is_dir():
        # Load all parquet files in directory
        parquet_files = list(targets_path.glob("*.parquet"))
        
        for file in tqdm(parquet_files, desc="Processing target files"):
            try:
                df = pd.read_parquet(file, columns=['id', 'approvedSymbol', 'biotype'])
                
                # Convert to records and load in batches
                records = df.to_dict(orient='records')
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = [
                        StagingOpenTargetsTarget(
                            id=record['id'],
                            approved_symbol=record.get('approvedSymbol'),
                            biotype=record.get('biotype')
                        )
                        for record in records[i:i+BATCH_SIZE]
                    ]
                    
                    session.add_all(batch)
                    session.commit()
            
            except Exception as e:
                logger.error(f"Error processing targets file {file}: {e}")
                session.rollback()
    
    # Extract diseases data
    logger.info(f"Extracting OpenTargets diseases data from {diseases_path}")
    diseases_path = Path(diseases_path)
    
    if diseases_path.is_dir():
        # Load all parquet files in directory
        parquet_files = list(diseases_path.glob("*.parquet"))
        
        for file in tqdm(parquet_files, desc="Processing disease files"):
            try:
                df = pd.read_parquet(file, columns=['id', 'name'])
                
                # Convert to records and load in batches
                records = df.to_dict(orient='records')
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = [
                        StagingOpenTargetsDisease(
                            id=record['id'],
                            name=record.get('name')
                        )
                        for record in records[i:i+BATCH_SIZE]
                    ]
                    
                    session.add_all(batch)
                    session.commit()
            
            except Exception as e:
                logger.error(f"Error processing diseases file {file}: {e}")
                session.rollback()
    
    # Extract associations data
    logger.info(f"Extracting OpenTargets associations data from {associations_path}")
    associations_path = Path(associations_path)
    
    if associations_path.is_dir():
        # Load all parquet files in directory
        parquet_files = list(associations_path.glob("*.parquet"))
        
        for file in tqdm(parquet_files, desc="Processing association files"):
            try:
                df = pd.read_parquet(file)
                
                # Convert to records and load in batches
                records = df.to_dict(orient='records')
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = [
                        StagingOpenTargetsAssociation(
                            target_id=record.get('targetId'),
                            disease_id=record.get('diseaseId'),
                            score=record.get('score'),
                            datasource=record.get('datasourceId')
                        )
                        for record in records[i:i+BATCH_SIZE]
                    ]
                    
                    session.add_all(batch)
                    session.commit()
            
            except Exception as e:
                logger.error(f"Error processing associations file {file}: {e}")
                session.rollback()
    
    # Get counts of loaded records
    targets_count = session.query(StagingOpenTargetsTarget).count()
    diseases_count = session.query(StagingOpenTargetsDisease).count()
    associations_count = session.query(StagingOpenTargetsAssociation).count()
    
    logger.info(f"Loaded {targets_count} targets, {diseases_count} diseases, and {associations_count} associations to staging")