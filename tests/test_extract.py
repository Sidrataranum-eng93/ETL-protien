# tests/test_extract.py
import os
import pytest
import tempfile
from lxml import etree
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.schema import Base, StagingUniProt, StagingString, StagingOpenTargetsTarget
from etl.extract import extract_uniprot_data, extract_string_data

SAMPLE_UNIPROT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="https://uniprot.org/uniprot" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://uniprot.org/uniprot http://www.uniprot.org/docs/uniprot.xsd">
  <entry dataset="Swiss-Prot" created="2025-01-01" modified="2025-05-01" version="1">
    <accession>P12345</accession>
    <name>TEST_HUMAN</name>
    <protein>
      <recommendedName>
        <fullName>Test Protein</fullName>
      </recommendedName>
    </protein>
    <gene>
      <name type="primary">TEST1</name>
    </gene>
    <organism>
      <name type="scientific">Homo sapiens</name>
      <name type="common">Human</name>
      <dbReference type="NCBI Taxonomy" id="9606"/>
      <lineage>
        <taxon>Eukaryota</taxon>
        <taxon>Metazoa</taxon>
        <taxon>Chordata</taxon>
        <taxon>Craniata</taxon>
        <taxon>Vertebrata</taxon>
        <taxon>Euteleostomi</taxon>
        <taxon>Mammalia</taxon>
        <taxon>Primates</taxon>
        <taxon>Hominidae</taxon>
        <taxon>Homo</taxon>
      </lineage>
    </organism>
    <comment type="function">
      <text>May play a role in test cellular processes and hypothetical protein interactions.</text>
    </comment>
    <comment type="similarity">
      <text>Belongs to the test protein family.</text>
    </comment>
    <dbReference type="STRING" id="9606.ENSP00000123456"/>
    <dbReference type="OpenTargets" id="ENSG00000123456"/>
    <dbReference type="Proteomes" id="UP000005640">
      <property type="component" value="Chromosome 1"/>
    </dbReference>
    <proteinExistence type="inferred from homology"/>
    <feature type="chain" id="PRO_0000000001" description="Test Protein Chain">
      <location>
        <begin position="1"/>
        <end position="100"/>
      </location>
    </feature>
    <sequence length="100" mass="12345" checksum="ABCDEF1234567890" modified="2025-01-01" version="1">ABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJ</sequence>
  </entry>
</uniprot>
"""

SAMPLE_STRING_DATA = """protein1 protein2 combined_score
9606.ENSP00000123456 9606.ENSP00000789012 900
9606.ENSP00000123456 9606.ENSP00000345678 150
"""

@pytest.fixture
def db_session():
    # Create in-memory database
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    # Clean up
    session.close()
    Base.metadata.drop_all(engine)

def test_extract_uniprot_data(db_session):
    # Create temporary file with sample data
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
        f.write(SAMPLE_UNIPROT_XML)
        temp_file = f.name
    
    try:
        # Extract data
        extract_uniprot_data(db_session, temp_file)
        
        # Verify results
        results = db_session.query(StagingUniProt).all()
        
        assert len(results) == 1
        
        protein = results[0]
        assert protein.accession == 'P12345'
        assert protein.protein_name == 'Test Protein'
        assert protein.gene_name == 'TEST1'
        assert protein.species == 'Human'
        assert protein.string_id == '9606.ENSP00000123456'
        assert protein.opentargets_id == 'ENSG00000123456'
        assert protein.sequence_length == 100
        assert protein.sequence_mass == 12345
    
    finally:
        # Clean up
        os.unlink(temp_file)

def test_extract_string_data(db_session):
    # Create temporary file with sample data
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as f:
        f.write(SAMPLE_STRING_DATA)
        temp_file = f.name
    
    try:
        # Extract data
        extract_string_data(db_session, temp_file)
        
        # Verify results
        results = db_session.query(StagingString).all()
        
        assert len(results) == 2
        
        # Check first interaction
        interaction1 = [i for i in results if i.protein1 == '9606.ENSP00000123456' and i.protein2 == '9606.ENSP00000789012'][0]
        assert interaction1.combined_score == 900
        
        # Check second interaction
        interaction2 = [i for i in results if i.protein1 == '9606.ENSP00000123456' and i.protein2 == '9606.ENSP00000345678'][0]
        assert interaction2.combined_score == 150
    
    finally:
        # Clean up
        os.unlink(temp_file)