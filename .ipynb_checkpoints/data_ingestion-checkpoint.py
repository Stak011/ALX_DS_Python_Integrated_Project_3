from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'

sql_query = """
SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""

weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
weather_mapping_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv"
### START FUNCTION

def create_db_engine(db_path):
    """
    create_db_engine creates SQLalchemy database engine using the provided database path

    Args:
        db_path (str): A string of database path
    Returns:
        returns established engine for SQLachemy
    Raises:
        ImportError: If SQLAchemy is not installed
        Exception: If any error occurs during engine creation, an exception with the error message is raised.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    query_data processes sqlite query, returns the appropriate data as DataFrame 
    using established sqlite engine
    
    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine to connect to
        sql_query (str): String of SQL querries to be executed
    Returns:
        pandas.DataFrame: Querried data in DataFRame
    Raises:
        VAlueError: If the SQL query returns an empty dataframe
        Exeption: If any error occurs during the sql execution, an exeption with the error message is raised
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    read_from_web_csv reads data from the provided URL, return it as a DataFrame
    Args:
        URL (str): String URL from which data is imported
    Returns:
        pandas.DataFrame: DataFrame of the read data
    Raises:
        pd.errors.EmptyDataError: If the read data is empty
        Exeption: If any error occurs during execution, exeption message of the error is raised
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION