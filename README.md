#  Protein Data Integration Pipeline

## Overview

The **Protein Data Integration Pipeline** consolidates protein information, interactions, and disease associations from **UniProt**, **STRING**, and **OpenTargets** into a structured database with a **three-layer architecture**: **Staging**, **Clean**, and **Semantic**. Designed to handle large biological datasets efficiently, it provides a robust and extensible ETL solution.

---

##  Features

* **Memory-efficient** processing of large datasets
* **Unified data model** for proteins, interactions, and diseases
* **Three-layer database architecture**: Staging → Clean → Semantic
* **Comprehensive error handling** and structured **logging**
* **Modular architecture** for extensibility and testing

---

##  Architecture

### 1. **Extraction Layer**

Fetches data from:

* **UniProt** (XML)
* **STRING** (TSV)
* **OpenTargets** (Parquet)

### 2. **Transformation Layer**

Performs:

* Data cleaning and validation
* Relationship modeling

### 3. **Loading Layer**

Populates the **SQLite database** across:

* `staging_*`
* `clean_*`
* `semantic_*` tables

---

##  Data Sources

| Source          | Format         | Description                        |
| --------------- | -------------- | ---------------------------------- |
| **UniProt**     | XML (\~8GB)    | Protein details                    |
| **STRING**      | TSV (multi-GB) | Protein-protein interaction scores |
| **OpenTargets** | Parquet        | Disease and target associations    |

---

##  Prerequisites

* Python 3.8+
* `pip install -r requirements.txt`
* Minimum **8GB RAM** and sufficient disk space
* Dependencies listed in `requirements.txt`

---

##  Installation

```bash
git clone https://github.com/username/protein-data-integration.git
cd protein-data-integration
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

##  Configuration

Edit `config.yaml` to define data paths and batch parameters:

```yaml
sources:
  uniprot:
    file_path: "data/uniprot_sprot.xml"
    batch_size: 1000
  string:
    file_path: "data/9606.protein.links.v11.5.txt.gz"
    score_threshold: 200
  opentargets:
    targets_file: "data/targets.parquet"
    diseases_file: "data/diseases.parquet"
    associations_file: "data/associations.parquet"

database:
  path: "protein_db.sqlite"
```

---

##  Usage

### Run Full ETL Pipeline

```bash
python main.py
```

### Run Specific Stages

```bash
python main.py --stage extract
python main.py --stage transform
python main.py --stage load
```

### Process Specific Data Sources Only

```bash
python main.py --uniprot-only
python main.py --string-only
python main.py --opentargets-only
```
---

##  Database Schema Overview

###  **Staging Layer**

| Table Name                        | Description                             |
| --------------------------------- | --------------------------------------- |
| `staging_uniprot`                 | Raw UniProt data                        |
| `staging_string`                  | STRING interactions with scores         |
| `staging_opentargets_target`      | OpenTargets protein targets             |
| `staging_opentargets_disease`     | OpenTargets disease metadata            |
| `staging_opentargets_association` | OpenTargets target-disease associations |

###  **Clean Layer**

| Table Name                    | Description                             |
| ----------------------------- | --------------------------------------- |
| `proteins`                    | Validated proteins                      |
| `protein_interactions`        | Normalized protein-protein interactions |
| `targets`                     | Cleaned target metadata                 |
| `diseases`                    | Cleaned disease metadata                |
| `target_disease_associations` | Cleaned disease associations            |

###  **Semantic Layer**

| Table Name                | Description                                                |
| ------------------------- | ---------------------------------------------------------- |
| `protein_disease_network` | Integrated protein-disease network with JSON relationships |

---

##  Schema Details (SQLAlchemy)

Example class from the staging layer:

```python
class StagingUniProt(Base):
    __tablename__ = 'staging_uniprot'
    accession = Column(String(20), primary_key=True)
    protein_name = Column(String(255))
    ...
```

Semantic Layer integrated view:

```python
class ProteinDiseaseNetwork(Base):
    __tablename__ = 'protein_disease_network'
    id = Column(Integer, primary_key=True, autoincrement=True)
    accession = Column(String(20))
    ...
    interacting_proteins = Column(Text)  # JSON array of interactions
```

For full schema details, see the `schema.py` file.

---

##  Database Initialization

```python
from schema import init_db
engine = init_db("sqlite:///protein_db.sqlite")
```

---

##  Testing

* Unit tests are located in the `tests/` folder.
* Run with:

```bash
pytest
```

---
