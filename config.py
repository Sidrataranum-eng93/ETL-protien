import os
from pathlib import Path

# Project structure
BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
TEMP_DIR = DATA_DIR / "temp"
LOG_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
for directory in [DATA_DIR, TEMP_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Database
DB_PATH = DATA_DIR / "protein_data.db"
DB_URI = f"sqlite:///{DB_PATH}"

# Source data files
UNIPROT_XML_PATH = DATA_DIR / "uniprot_sprot.xml"
STRING_DATA_PATH = DATA_DIR / "9606.protein.links.v12.0.txt"
OPENTARGETS_TARGETS_PATH = DATA_DIR / "target"
OPENTARGETS_DISEASES_PATH = DATA_DIR / "disease"
OPENTARGETS_ASSOCIATIONS_PATH = DATA_DIR / "association_by_datasource_direct"

# Thresholds
STRING_SCORE_THRESHOLD = 200

# Batch size for processing
BATCH_SIZE = 1000