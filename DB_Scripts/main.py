import logging
from Table_Creation_Script import create_tables
from Ingestion_Script import ingest_imf_values, ingest_imf_indicators

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("imf_data_processing.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    logger.info("Starting IMF data ingestion")

    try:
        logger.info("Creating database tables")
        create_tables()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        return

    try:
        logger.info("Ingesting IMF values")
        ingest_imf_values()
        logger.info("IMF values ingested successfully")
    except Exception as e:
        logger.error(f"Error ingesting IMF values: {e}")

    try:
        logger.info("Ingesting IMF indicators")
        ingest_imf_indicators()
        logger.info("IMF indicators ingested successfully")
    except Exception as e:
        logger.error(f"Error ingesting IMF indicators: {e}")

    logger.info("IMF data processing completed")

if __name__ == "__main__":
    main()