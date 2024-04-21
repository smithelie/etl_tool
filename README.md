# ETL Tool Package
## Overview

The etl_tool package is designed to facilitate the extraction, transformation and 
loading of data across different sources and formats into structured data storage 
systems for analytical purposes.
The package supports various data sources including files, databases and 
web-based APIs and it provides common data transformations as well as loading
to both local storage and cloud-based storage solutions like AWS S3.

## Repository Structure
```
../etl_tool -- package root directory
    ../config/ -- contains configuration files used by the APIExtract class
        - open_weather_credentials.json
        - reddit_credentials.json
    ../data/ -- contains sample data sets used by the FileExtract and DatabaseExtract classes
        ../outbox/ -- Output data directory for the test_load.py module
            - data_written_by_file_loader.csv
        - database.db -- sample database for use with the test_extract.py module
        - json.json -- sample JSON for use with the test_extract.py module
        - tabular.csv -- sample tabular data for use with the test_extract.py module
        - tabular_with_array_column.csv -- sample tabular data with array column for use with the test_extract.py module
    - __init__.py -- initializes etl_tool as a package
    - extract.py -- contains classes for extracting data from files, databases and APIs
    - transform.py -- contains a class with methods covering common data transformations seen in data pipelines
    - load.py -- contains a class with methods that enable writing data to local storage or AWS S3 buckets
    - test_extract.py -- contains a suite of tests for the extract.py module
    - test_transform.py -- contains a suite of tests for the transform.py module
    - test_load.py -- contains a suite of tests for the load.py module
    - build_test_database.py -- builds a sample SQLite database for use by the test_extract.py module
    - README.md - self
```

## Sample Usage

### Extracting Data
```
# Initialize FileExtract with file path
file_extractor = FileExtract('/home/smith/Development/ds5010/github/etl_tool/data/tabular_with_array_column.csv')
# Get data into dataframe
raw_data = file_extractor.read_tabular(delimiter=',')
```

### Transforming Data
```
# Initialize FileExtract with file path
file_extractor = FileExtract('/home/smith/Development/ds5010/github/etl_tool/data/tabular_with_array_column.csv')
# Get data into dataframe
raw_data = file_extractor.read_tabular(delimiter=',')
# Initialize Transformer with raw data
transformer = Transformer(raw_data=raw_data)
# Explode phone_numbers column
transformer.explode_column(col='phone_numbers', delimiter='|')
processed_data = transformer.get_data()
```

### Loading Data
```
# Initialize FileExtract with file path
file_extractor = FileExtract('/home/smith/Development/ds5010/github/etl_tool/data/tabular_with_array_column.csv')
# Get data into dataframe
raw_data = file_extractor.read_tabular(delimiter=',')
# Initialize Transformer with raw data
transformer = Transformer(raw_data=raw_data)
# Explode phone_numbers column
transformer.explode_column(col='phone_numbers', delimiter='|')
processed_data = transformer.get_data()
# Intialize Loader with processed data
file_loader = Load(data=processed_data)
file_loader.write_to_file(path='.') # write to current working directory
```


