from sqlalchemy import Column, String, Integer, Float, ForeignKey, Table, Text, create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class StagingUniProt(Base):
    __tablename__ = 'staging_uniprot'
    
    accession = Column(String(20), primary_key=True)
    protein_name = Column(String(255))
    gene_name = Column(String(50))
    species = Column(String(100))
    string_id = Column(String(50))
    opentargets_id = Column(String(50))
    sequence_length = Column(Integer)
    sequence_mass = Column(Float)
    sequence = Column(Text)

class StagingString(Base):
    __tablename__ = 'staging_string'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    protein1 = Column(String(50))
    protein2 = Column(String(50))
    combined_score = Column(Integer)

class StagingOpenTargetsTarget(Base):
    __tablename__ = 'staging_opentargets_target'
    
    id = Column(String(50), primary_key=True)
    approved_symbol = Column(String(50))
    biotype = Column(String(50))

class StagingOpenTargetsDisease(Base):
    __tablename__ = 'staging_opentargets_disease'
    
    id = Column(String(50), primary_key=True)
    name = Column(String(255))

class StagingOpenTargetsAssociation(Base):
    __tablename__ = 'staging_opentargets_association'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    target_id = Column(String(50))
    disease_id = Column(String(50))
    score = Column(Float)
    datasource = Column(String(50))

# Intermediate layer
class Protein(Base):
    __tablename__ = 'proteins'
    
    accession = Column(String(20), primary_key=True)
    protein_name = Column(String(255))
    gene_name = Column(String(50))
    species = Column(String(100))
    string_id = Column(String(50))
    opentargets_id = Column(String(50))
    sequence_length = Column(Integer)
    sequence_mass = Column(Float)

class ProteinInteraction(Base):
    __tablename__ = 'protein_interactions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    protein1 = Column(String(20), ForeignKey('proteins.accession'))
    protein2 = Column(String(20), ForeignKey('proteins.accession'))
    combined_score = Column(Integer)

class Target(Base):
    __tablename__ = 'targets'
    
    id = Column(String(50), primary_key=True)
    approved_symbol = Column(String(50))
    biotype = Column(String(50))
    uniprot_accession = Column(String(20), ForeignKey('proteins.accession'), nullable=True)

class Disease(Base):
    __tablename__ = 'diseases'
    
    id = Column(String(50), primary_key=True)
    name = Column(String(255))

class TargetDiseaseAssociation(Base):
    __tablename__ = 'target_disease_associations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    target_id = Column(String(50), ForeignKey('targets.id'))
    disease_id = Column(String(50), ForeignKey('diseases.id'))
    score = Column(Float)
    datasource = Column(String(50))

# Semantic layer
class ProteinDiseaseNetwork(Base):
    __tablename__ = 'protein_disease_network'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    accession = Column(String(20))
    protein_name = Column(String(255))
    gene_name = Column(String(50))
    species = Column(String(100))
    sequence_length = Column(Integer)
    sequence_mass = Column(Float)
    disease_id = Column(String(50))
    disease_name = Column(String(255))
    association_score = Column(Float)
    interacting_proteins = Column(Text)  # JSON serialized list of strongly associated proteins

def init_db(db_uri):
    """Initialize the database with the schema"""
    engine = create_engine(db_uri)
    Base.metadata.create_all(engine)
    return engine