# tests/test_load.py
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.schema import (
    Base, Protein, ProteinInteraction, Target, Disease, TargetDiseaseAssociation,
    ProteinDiseaseNetwork
)
from etl.load import build_semantic_layer

@pytest.fixture
def db_session():
    # Create in-memory database
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Add test data
    # Proteins
    protein1 = Protein(
        accession='P12345',
        protein_name='Test Protein 1',
        gene_name='TEST1',
        species='Human',
        string_id='9606.ENSP00000123456',
        opentargets_id='ENSG00000123456',
        sequence_length=100,
        sequence_mass=12345
    )
    
    protein2 = Protein(
        accession='P67890',
        protein_name='Test Protein 2',
        gene_name='TEST2',
        species='Human',
        string_id='9606.ENSP00000789012',
        opentargets_id='ENSG00000789012',
        sequence_length=200,
        sequence_mass=23456
    )
    
    # Interactions
    interaction = ProteinInteraction(
        protein1='P12345',
        protein2='P67890',
        combined_score=900
    )
    
    # Targets
    target1 = Target(
        id='ENSG00000123456',
        approved_symbol='TEST1',
        biotype='protein_coding',
        uniprot_accession='P12345'
    )
    
    target2 = Target(
        id='ENSG00000789012',
        approved_symbol='TEST2',
        biotype='protein_coding',
        uniprot_accession='P67890'
    )
    
    # Diseases
    disease1 = Disease(
        id='EFO:0000001',
        name='Test Disease 1'
    )
    
    disease2 = Disease(
        id='EFO:0000002',
        name='Test Disease 2'
    )
    
    # Associations
    association1 = TargetDiseaseAssociation(
        target_id='ENSG00000123456',
        disease_id='EFO:0000001',
        score=0.8,
        datasource='genetic_association'
    )
    
    association2 = TargetDiseaseAssociation(
        target_id='ENSG00000789012',
        disease_id='EFO:0000002',
        score=0.7,
        datasource='genetic_association'
    )
    
    session.add_all([protein1, protein2, interaction, target1, target2, disease1, disease2, association1, association2])
    session.commit()
    
    yield session
    
    # Clean up
    session.close()
    Base.metadata.drop_all(engine)

def test_build_semantic_layer(db_session):
    # Build semantic layer
    build_semantic_layer(db_session)
    
    # Verify results
    results = db_session.query(ProteinDiseaseNetwork).all()
    
    assert len(results) == 2
    
    # Check first entry
    entry1 = db_session.query(ProteinDiseaseNetwork).filter(
        ProteinDiseaseNetwork.accession == 'P12345'
    ).first()
    
    assert entry1.protein_name == 'Test Protein 1'
    assert entry1.gene_name == 'TEST1'
    assert entry1.disease_id == 'EFO:0000001'
    assert entry1.disease_name == 'Test Disease 1'
    assert entry1.association_score == 0.8
    
    # Check interacting proteins in JSON
    interacting_proteins = json.loads(entry1.interacting_proteins)
    assert len(interacting_proteins) == 1
    assert interacting_proteins[0]['accession'] == 'P67890'
    assert interacting_proteins[0]['protein_name'] == 'Test Protein 2'
    
    # Check second entry
    entry2 = db_session.query(ProteinDiseaseNetwork).filter(
        ProteinDiseaseNetwork.accession == 'P67890'
    ).first()
    
    assert entry2.protein_name == 'Test Protein 2'
    assert entry2.gene_name == 'TEST2'
    assert entry2.disease_id == 'EFO:0000002'
    assert entry2.disease_name == 'Test Disease 2'
    assert entry2.association_score == 0.7
    
    # Check interacting proteins in JSON
    interacting_proteins = json.loads(entry2.interacting_proteins)
    assert len(interacting_proteins) == 1
    assert interacting_proteins[0]['accession'] == 'P12345'
    assert interacting_proteins[0]['protein_name'] == 'Test Protein 1'