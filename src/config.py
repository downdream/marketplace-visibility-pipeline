from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"
DOCS_DIR = BASE_DIR / "docs"
SAMPLE_DATA_DIR = BASE_DIR / "sample_data"

DB_NAME = os.getenv("DB_NAME", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH")
BACKMARKET_API_KEY = os.getenv("BACKMARKET_API_KEY", "REPLACE_ME")
BACKMARKET_APP_ID = os.getenv("BACKMARKET_APP_ID", "9X8ZUDUNN9")
KAUFLAND_CLIENT_KEY = os.getenv("KAUFLAND_CLIENT_KEY")
KAUFLAND_SECRET_KEY = os.getenv("KAUFLAND_SECRET_KEY")