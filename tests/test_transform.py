# tests/test_transform.py
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.schema import (
    Base, StagingUniProt, StagingString, StagingOpenTargetsTarget, StagingOpenTargetsDisease,
    StagingOpenTargetsAssociation, Protein, ProteinInteraction, Target, Disease, TargetDiseaseAssociation
)
from etl.transform import transform_uniprot_to_proteins, transform_string_to_interactions, transform_opentargets_data

@pytest.fixture
def db_session():
    # Create in-memory database
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Add test data
    # UniProt test data
    uniprot1 = StagingUniProt(
        accession='P12345',
        protein_name='Test Protein 1',
        gene_name='TEST1',
        species='Human',
        string_id='9606.ENSP00000123456',
        opentargets_id='ENSG00000123456',
        sequence_length=100,
        sequence_mass=12345
    )
    
    uniprot2 = StagingUniProt(
        accession='P67890',
        protein_name='Test Protein 2',
        gene_name='TEST2',
        species='Human',
        string_id='9606.ENSP00000789012',
        opentargets_id='ENSG00000789012',
        sequence_length=200,
        sequence_mass=23456
    )
    
    # STRING test data
    string1 = StagingString(
        protein1='9606.ENSP00000123456',
        protein2='9606.ENSP00000789012',
        combined_score=900
    )
    
    string2 = StagingString(
        protein1='9606.ENSP00000123456',
        protein2='9606.ENSP00000345678',
        combined_score=150
    )
    
    # OpenTargets test data
    target1 = StagingOpenTargetsTarget(
        id='ENSG00000123456',
        approved_symbol='TEST1',
        biotype='protein_coding'
    )
    
    target2 = StagingOpenTargetsTarget(
        id='ENSG00000789012',
        approved_symbol='TEST2',
        biotype='protein_coding'
    )
    
    disease1 = StagingOpenTargetsDisease(
        id='EFO:0000001',
        name='Test Disease 1'
    )
    
    disease2 = StagingOpenTargetsDisease(
        id='EFO:0000002',
        name='Test Disease 2'
    )
    
    association1 = StagingOpenTargetsAssociation(
        target_id='ENSG00000123456',
        disease_id='EFO:0000001',
        score=0.8,
        datasource='genetic_association'
    )
    
    association2 = StagingOpenTargetsAssociation(
        target_id='ENSG00000789012',
        disease_id='EFO:0000002',
        score=0.7,
        datasource='genetic_association'
    )
    
    session.add_all([uniprot1, uniprot2, string1, string2, target1, target2, disease1, disease2, association1, association2])
    session.commit()
    
    yield session
    
    # Clean up
    session.close()
    Base.metadata.drop_all(engine)

def test_transform_uniprot_to_proteins(db_session):
    # Transform data
    transform_uniprot_to_proteins(db_session)
    
    # Verify results
    results = db_session.query(Protein).all()
    
    assert len(results) == 2
    
    # Check first protein
    protein1 = db_session.query(Protein).filter(Protein.accession == 'P12345').first()
    assert protein1.protein_name == 'Test Protein 1'
    assert protein1.gene_name == 'TEST1'
    assert protein1.species == 'Human'
    assert protein1.string_id == '9606.ENSP00000123456'
    assert protein1.opentargets_id == 'ENSG00000123456'
    assert protein1.sequence_length == 100
    assert protein1.sequence_mass == 12345
    
    # Check second protein
    protein2 = db_session.query(Protein).filter(Protein.accession == 'P67890').first()
    assert protein2.protein_name == 'Test Protein 2'
    assert protein2.gene_name == 'TEST2'
    assert protein2.species == 'Human'
    assert protein2.string_id == '9606.ENSP00000789012'
    assert protein2.opentargets_id == 'ENSG00000789012'
    assert protein2.sequence_length == 200
    assert protein2.sequence_mass == 23456

def test_transform_string_to_interactions(db_session):
    # First transform UniProt data to populate Protein table
    transform_uniprot_to_proteins(db_session)
    
    # Transform STRING data
    transform_string_to_interactions(db_session)
    
    # Verify results
    results = db_session.query(ProteinInteraction).all()
    
    # Only one interaction should be above threshold (900 > 200)
    assert len(results) == 1
    
    interaction = results[0]
    assert interaction.protein1 == 'P12345'
    assert interaction.protein2 == 'P67890'
    assert interaction.combined_score == 900

def test_transform_opentargets_data(db_session):
    # First transform UniProt data to populate Protein table
    transform_uniprot_to_proteins(db_session)
    
    # Transform OpenTargets data
    transform_opentargets_data(db_session)
    
    # Verify results
    targets = db_session.query(Target).all()
    diseases = db_session.query(Disease).all()
    associations = db_session.query(TargetDiseaseAssociation).all()
    
    assert len(targets) == 2
    assert len(diseases) == 2
    assert len(associations) == 2
    
    # Check target-UniProt links
    target1 = db_session.query(Target).filter(Target.id == 'ENSG00000123456').first()
    assert target1.approved_symbol == 'TEST1'
    assert target1.uniprot_accession == 'P12345'
    
    target2 = db_session.query(Target).filter(Target.id == 'ENSG00000789012').first()
    assert target2.approved_symbol == 'TEST2'
    assert target2.uniprot_accession == 'P67890'
    
    # Check associations
    assoc1 = db_session.query(TargetDiseaseAssociation).filter(
        TargetDiseaseAssociation.target_id == 'ENSG00000123456'
    ).first()
    assert assoc1.disease_id == 'EFO:0000001'
    assert assoc1.score == 0.8
    
    assoc2 = db_session.query(TargetDiseaseAssociation).filter(
        TargetDiseaseAssociation.target_id == 'ENSG00000789012'
    ).first()
    assert assoc2.disease_id == 'EFO:0000002'
    assert assoc2.score == 0.7