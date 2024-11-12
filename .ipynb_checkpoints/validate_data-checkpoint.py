import pytest
import re
import numpy as np
import pandas as pd
from data_ingestion import read_from_web_CSV, query_data, create_db_engine
from field_data_processor import FieldDataProcessor
from weather_data_processor import WeatherDataProcessor
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

config_params = {
        "sql_query": """
        SELECT *
        FROM geographic_features
        LEFT JOIN weather_features USING (Field_ID)
        LEFT JOIN soil_and_crop_features USING (Field_ID)
        LEFT JOIN farm_management_features USING (Field_ID)
            """, # Insert your SQL query
    "db_path": "sqlite:///Maji_Ndogo_farm_survey_small.db", # Insert the db_path of the database
    "columns_to_rename": {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'}, # Insert the disctionary of columns we want to swop the names of, 
    "values_to_rename": {'cassaval': 'cassava', 'wheatn': 'wheat', 'teaa': 'tea', 'cassava ':'cassava','wheat ':'wheat','tea ':'tea'},
    "weather_map_data": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv", # Insert the weather data mapping CSV here

    # Add two new keys
    "weather_csv_path": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv", # Insert the weather data CSV here
    "regex_patterns" : {
    'Rainfall': r'(\d+(\.\d+)?)\s?mm',
     'Temperature': r'(\d+(\.\d+)?)\s?C',
    'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
    } # Insert the regex pattern we used to process the messages
}

field_processor = FieldDataProcessor(config_params)
field_processor.process()
field_df = field_processor.df

weather_processor = WeatherDataProcessor(config_params)
weather_processor.process()
weather_df = weather_processor.weather_df

# Test functions

def test_read_weather_DataFrame_shape():
    weather_df = read_from_web_CSV(config_params["weather_csv_path"])
    expected_shape = (len(weather_df), len(weather_df.columns))
    assert weather_df.shape == expected_shape

def test_read_field_DataFrame_shape():
    field_df = query_data(create_db_engine(config_params["db_path"]), config_params["sql_query"])
    expected_shape = (len(field_df), len(field_df.columns))
    assert field_df.shape == expected_shape

def test_weather_DataFrame_columns():
    weather_df = read_from_web_CSV(config_params["weather_csv_path"])
    expected_columns = ['Weather_station_ID', 'Message']  # Replace with actual column names
    assert list(weather_df.columns) == expected_columns

def test_field_DataFrame_columns():
    field_df = query_data(create_db_engine(config_params["db_path"]), config_params["sql_query"])
    expected_columns = ['Field_ID','Elevation','Latitude','Longitude','Location','Slope','Rainfall','Min_temperature_C','Max_temperature_C','Ave_temps','Soil_fertility','Soil_type','pH','Pollution_level','Plot_size','Crop_type','Annual_yield','Standard_yield']
    assert list(field_df.columns) == expected_columns

def test_field_DataFrame_non_negative_elevation():
    field_df = query_data(create_db_engine(config_params["db_path"]), config_params["sql_query"])
    #field_df['Elevation'] = field_df['Elevation'].abs()
    field_df = field_processor.apply_corrections()
    assert (field_df['Elevation'] >= 0).all()

def test_crop_types_are_valid():
    valid_crop_types = ['cassava', 'tea', 'wheat', 'potato', 'banana', 'coffee', 'rice', 'maize']
    field_df = query_data(create_db_engine(config_params["db_path"]), config_params["sql_query"])
    field_df = field_processor.apply_corrections()
    #['Crop_type'], field_df['Annual_yield'] = field_df['Annual_yield'], field_df['Crop_type']
    assert field_df['Crop_type'].isin(valid_crop_types)

def test_positive_rainfall_values():
    weather_df = read_from_web_CSV(config_params["weather_csv_path"])
    assert 'Rainfall' in weather_df.columns
    assert (weather_df['Rainfall'] >= 0).all()

field_processor = FieldDataProcessor(config_params)
field_processor.process()
field_df = field_processor.df

weather_processor = WeatherDataProcessor(config_params)
weather_processor.process()
weather_df = weather_processor.weather_df

# Run tests
#if __name__ == '__main__':
    #pytest.main(['validate_data.py', '-v'])