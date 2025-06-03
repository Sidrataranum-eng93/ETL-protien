# etl/load.py
import json
import logging
from sqlalchemy import text
from tqdm import tqdm
from tabulate import tabulate  # NEW import for tabular display

from models.schema import ProteinDiseaseNetwork
from config import BATCH_SIZE, STRING_SCORE_THRESHOLD

logger = logging.getLogger(__name__)

def build_semantic_layer(session):
    """
    Build the semantic layer by combining data from the clean tables.
    Optimized for performance when dealing with large datasets.
    
    Args:
        session: SQLAlchemy session
    """
    logger.info("Building semantic layer")
    
    # Clear existing data
    session.query(ProteinDiseaseNetwork).delete()
    session.commit()
    
    # Get all proteins with OpenTargets links
    logger.info("Gathering proteins with OpenTargets links")
    protein_query = """
    SELECT p.accession, p.protein_name, p.gene_name, p.species,
           p.sequence_length, p.sequence_mass, p.opentargets_id
    FROM proteins p
    WHERE p.opentargets_id IS NOT NULL
    """
    
    proteins = session.execute(text(protein_query)).fetchall()
    logger.info(f"Found {len(proteins)} proteins with OpenTargets links")
    
    # Process proteins in batches
    batch_size = 100
    total_proteins = len(proteins)
    processed = 0
    
    for i in range(0, total_proteins, batch_size):
        batch_proteins = proteins[i:i+batch_size]
        batch_records = []
        
        # Prepare a list of all protein accessions in this batch
        batch_accessions = [p[0] for p in batch_proteins]
        accession_list = "', '".join(batch_accessions)
        
        # Get disease associations for all proteins in batch
        if batch_accessions:
            disease_query = f"""
            SELECT t.uniprot_accession, d.id, d.name, a.score
            FROM target_disease_associations a
            JOIN diseases d ON a.disease_id = d.id
            JOIN targets t ON a.target_id = t.id
            WHERE t.uniprot_accession IN ('{accession_list}')
            """
            
            disease_assocs = session.execute(text(disease_query)).fetchall()
            
            # Group by protein accession
            disease_map = {}
            for acc, d_id, d_name, score in disease_assocs:
                if acc not in disease_map:
                    disease_map[acc] = []
                disease_map[acc].append((d_id, d_name, score))
            
            # Get interacting proteins for all proteins in batch
            interact_query = f"""
            WITH combined_interactions AS (
                SELECT protein1 as acc1, protein2 as acc2
                FROM protein_interactions
                WHERE protein1 IN ('{accession_list}')
                AND combined_score > {STRING_SCORE_THRESHOLD}
                UNION
                SELECT protein2 as acc1, protein1 as acc2
                FROM protein_interactions
                WHERE protein2 IN ('{accession_list}')
                AND combined_score > {STRING_SCORE_THRESHOLD}
            )
            SELECT ci.acc1, p.accession, p.protein_name, p.gene_name
            FROM combined_interactions ci
            JOIN proteins p ON ci.acc2 = p.accession
            """
            
            interactions = session.execute(text(interact_query)).fetchall()
            
            # Group by protein accession
            interact_map = {}
            for acc1, acc2, p_name, g_name in interactions:
                if acc1 not in interact_map:
                    interact_map[acc1] = []
                interact_map[acc1].append({
                    'accession': acc2,
                    'protein_name': p_name,
                    'gene_name': g_name
                })
        
        # Process each protein in the batch
        for protein in batch_proteins:
            accession, protein_name, gene_name, species, seq_length, seq_mass, opentargets_id = protein
            
            # Get disease associations
            protein_diseases = disease_map.get(accession, [])
            
            # Get interacting proteins
            protein_interactions = interact_map.get(accession, [])
            interacting_proteins_json = json.dumps(protein_interactions)
            
            # Create records
            if protein_diseases:
                for disease_id, disease_name, score in protein_diseases:
                    network_entry = ProteinDiseaseNetwork(
                        accession=accession,
                        protein_name=protein_name,
                        gene_name=gene_name,
                        species=species,
                        sequence_length=seq_length,
                        sequence_mass=seq_mass,
                        disease_id=disease_id,
                        disease_name=disease_name,
                        association_score=score,
                        interacting_proteins=interacting_proteins_json
                    )
                    batch_records.append(network_entry)
            else:
                # No disease associations
                network_entry = ProteinDiseaseNetwork(
                    accession=accession,
                    protein_name=protein_name,
                    gene_name=gene_name,
                    species=species,
                    sequence_length=seq_length,
                    sequence_mass=seq_mass,
                    disease_id=None,
                    disease_name=None,
                    association_score=None,
                    interacting_proteins=interacting_proteins_json
                )
                batch_records.append(network_entry)
        
        # Insert the batch
        if batch_records:
            session.add_all(batch_records)
            session.commit()
        
        processed += len(batch_proteins)
        logger.info(f"Processed {processed}/{total_proteins} proteins")
    
    # Get count of semantic layer records
    count = session.query(ProteinDiseaseNetwork).count()
    logger.info(f"Built semantic layer with {count} protein-disease network entries")

    # Display sample records as a table
    sample_records = session.query(ProteinDiseaseNetwork).limit(5).all()
    table_data = [
        [
            r.accession,
            r.protein_name,
            r.gene_name,
            r.species,
            r.sequence_length,
            r.sequence_mass,
            r.disease_name,
            r.association_score,
            len(json.loads(r.interacting_proteins)) if r.interacting_proteins else 0
        ]
        for r in sample_records
    ]
    headers = [
        "Accession", "Protein Name", "Gene Name", "Species",
        "Seq Length", "Seq Mass", "Disease", "Score", "Interactors"
    ]
    print("\nSample Records from ProteinDiseaseNetwork:")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
