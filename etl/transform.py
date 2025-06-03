# etl/transform.py
import json
import logging
from sqlalchemy import text
from tqdm import tqdm

from models.schema import (
    Protein, ProteinInteraction, Target, Disease, TargetDiseaseAssociation
)
from config import BATCH_SIZE, STRING_SCORE_THRESHOLD

logger = logging.getLogger(__name__)

def transform_uniprot_to_proteins(session):
    """
    Transform data from staging_uniprot to proteins table
    
    Args:
        session: SQLAlchemy session
    """
    logger.info("Transforming UniProt data to proteins table")
    
    # Clear existing data
    session.query(Protein).delete()
    session.commit()
    
    # Transfer data from staging to clean layer
    query = """
    INSERT INTO proteins (accession, protein_name, gene_name, species, string_id, 
                         opentargets_id, sequence_length, sequence_mass)
    SELECT accession, protein_name, gene_name, species, string_id, 
           opentargets_id, sequence_length, sequence_mass
    FROM staging_uniprot
    """
    
    session.execute(text(query))
    session.commit()
    
    # Get count of transformed records
    count = session.query(Protein).count()
    logger.info(f"Transformed {count} protein records")

def transform_string_to_interactions(session):
    """
    Transform data from staging_string to protein_interactions table
    
    Args:
        session: SQLAlchemy session
    """
    logger.info("Transforming STRING data to protein_interactions table")
    
    # Clear existing data
    session.query(ProteinInteraction).delete()
    session.commit()
    
    # Get all protein accessions and their STRING IDs
    protein_string_ids = dict(session.query(Protein.accession, Protein.string_id)
                             .filter(Protein.string_id.isnot(None))
                             .all())
    
    # Create mapping from STRING ID to UniProt accession
    string_to_uniprot = {v: k for k, v in protein_string_ids.items() if v}
    
    # Get STRING interactions above threshold
    interactions_query = f"""
    SELECT protein1, protein2, combined_score
    FROM staging_string
    WHERE combined_score > {STRING_SCORE_THRESHOLD}
    """
    
    interactions = session.execute(text(interactions_query)).fetchall()
    logger.info(f"Found {len(interactions)} STRING interactions above threshold")
    
    # Process in batches
    batch = []
    
    for protein1, protein2, score in tqdm(interactions, desc="Processing interactions"):
        # Convert STRING IDs to UniProt accessions
        uniprot1 = string_to_uniprot.get(protein1)
        uniprot2 = string_to_uniprot.get(protein2)
        
        # Only include interactions where both proteins are in UniProt
        if uniprot1 and uniprot2:
            interaction = ProteinInteraction(
                protein1=uniprot1,
                protein2=uniprot2,
                combined_score=score
            )
            
            batch.append(interaction)
            
            # Process in batches
            if len(batch) >= BATCH_SIZE:
                session.add_all(batch)
                session.commit()
                batch = []
    
    # Add any remaining records
    if batch:
        session.add_all(batch)
        session.commit()
    
    # Get count of transformed records
    count = session.query(ProteinInteraction).count()
    logger.info(f"Transformed {count} protein interaction records")

def transform_opentargets_data(session):
    """
    Transform data from OpenTargets staging tables to clean tables
    
    Args:
        session: SQLAlchemy session
    """
    logger.info("Transforming OpenTargets data")
    
    # Clear existing data
    session.query(TargetDiseaseAssociation).delete()
    session.query(Disease).delete()
    session.query(Target).delete()
    session.commit()
    
    # Transform targets
    logger.info("Transforming targets")
    
    # Copy targets to clean table
    target_query = """
    INSERT INTO targets (id, approved_symbol, biotype)
    SELECT id, approved_symbol, biotype
    FROM staging_opentargets_target
    """
    
    session.execute(text(target_query))
    session.commit()
    
    # Link targets to UniProt proteins
    link_query = """
    UPDATE targets
    SET uniprot_accession = (
        SELECT accession
        FROM proteins
        WHERE proteins.opentargets_id = targets.id
    )
    WHERE EXISTS (
        SELECT 1
        FROM proteins
        WHERE proteins.opentargets_id = targets.id
    )
    """
    
    session.execute(text(link_query))
    session.commit()
    
    # Transform diseases
    logger.info("Transforming diseases")
    
    # Copy diseases to clean table
    disease_query = """
    INSERT INTO diseases (id, name)
    SELECT id, name
    FROM staging_opentargets_disease
    """
    
    session.execute(text(disease_query))
    session.commit()
    
    # Transform associations
    logger.info("Transforming target-disease associations")
    
    # Copy associations to clean table
    assoc_query = """
    INSERT INTO target_disease_associations (target_id, disease_id, score, datasource)
    SELECT target_id, disease_id, score, datasource
    FROM staging_opentargets_association
    """
    
    session.execute(text(assoc_query))
    session.commit()
    
    # Get counts of transformed records
    targets_count = session.query(Target).count()
    diseases_count = session.query(Disease).count()
    associations_count = session.query(TargetDiseaseAssociation).count()
    linked_targets_count = session.query(Target).filter(Target.uniprot_accession.isnot(None)).count()
    
    logger.info(f"Transformed {targets_count} targets ({linked_targets_count} linked to UniProt), "
               f"{diseases_count} diseases, and {associations_count} associations")