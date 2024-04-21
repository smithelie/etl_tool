'''
Smith Elie
DS5010: test_extract
A suite of tests for testing the extract module classes.
TestFileExtract:
    - User must update [path_to_tabular_data] and [path_to_json_data] to match their system's absolute path to the cloned repository.
TestDatabaseExtract:
    - User must update [connection_url] to match their system's absolute path to the cloned repository.
TestAPIExtract:
    - User must update [path_to_reddit_config_file] and [path_to_openweather_config_file] to match their system's
      absolute path to the cloned repository.
    - Users must also provide their own credentials to test functionality of the APIExtract class.
        -- Sign up for openweather API credentials: https://openweathermap.org/api
        -- Sign up for reddit API credentials for use with PRAW (Python Reddit API Wrapper): https://praw.readthedocs.io/en/stable/getting_started/quick_start.html
'''

import unittest
from extract import FileExtract, DatabaseExtract, APIExtract


class TestFileExtract(unittest.TestCase):
    '''
    Tests the FileExtract class methods.
    User must update [path_to_tabular_data] and [path_to_json_data] to match their system's absolute path to the cloned repository.
    '''
    path_to_tabular_data = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/tabular.csv' # user must update
    path_to_json_data = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/json.json' # user must update

    def test_init_with_file_path(self):
        '''
        Tests the initialization of the FileExtract class
        Ensure attribute assignment works as expected
        '''
        file_extractor = FileExtract(self.path_to_tabular_data)
        self.assertEqual(self.path_to_tabular_data, file_extractor.file_path)

    def test_read_tabular(self):
        '''
        Tests the read_tabular method of the FileExtract class
        Ensure tabular data is read and contains the expected number of rows and columns
        '''
        file_extractor = FileExtract()
        tabular_data = file_extractor.read_tabular(delimiter=',', file_path=self.path_to_tabular_data)
        self.assertEqual((32, 12), tabular_data.shape)  # expected vs actual

    def test_read_json(self):
        '''
        Tests the read_json method of the FileExtract
        Ensure the extracted JSON contains the expected keys
        '''
        file_extractor = FileExtract()
        json_data = file_extractor.read_json(file_path=self.path_to_json_data)
        self.assertEqual(['id', 'name', 'email', 'address', 'orders'], list(json_data.keys()))  # expected vs actual


class TestDatabaseExtract(unittest.TestCase):
    '''
    Tests the DatabaseExtract class methods.
    User must update [connection_url] to match their system's absolute path to the cloned repository.
    [connection_str] uses a relative path so updating this is optional.
    '''
    connection_str = 'DRIVER=SQLite3;DATABASE=data/database.db'  # user can update (relative path used)
    connection_url = 'sqlite://///home/smith/Development/ds5010/github/ds5010/etl_tool/data/database.db'  # user must update

    def test_init_with_conn_str(self):
        '''
        Tests the init_with_conn_str method of the DatabaseExtract class
        Ensures that when supplied a connection_str, the type of connection is pyodbc
        '''
        database_extractor = DatabaseExtract(connection_str=self.connection_str)
        self.assertEqual('pyodbc', database_extractor.connection_type)  # expected vs actual. Assignment test.

    def test_init_with_conn_url(self):
        '''
        Tests the init_with_conn_url method of the DatabaseExtract class
        Ensures that when supplied a connection_url, the type of connection is sqlalchemy
        '''
        database_extractor = DatabaseExtract(connection_url=self.connection_url)
        self.assertEqual('sqlalchemy', database_extractor.connection_type)  # expected vs actual. Assignment test.

    def test_query_with_conn_str(self):
        '''
        Tests the query_with_conn_str method of the DatabaseExtract class
        Ensure the queried data has the expected number of rows and columns
        '''
        database_extractor = DatabaseExtract(connection_str=self.connection_str)
        data_via_pyodbc = database_extractor.query('SELECT * FROM people LIMIT 3')
        self.assertEqual((3, 3), data_via_pyodbc.shape)

    def test_query_with_conn_url(self):
        '''
        Tests the query_with_conn_str method of the DatabaseExtract class
        Ensure the queried data has the expected number of rows and columns
        '''
        database_extractor = DatabaseExtract(connection_url=self.connection_url)
        data_via_sql_alchemy = database_extractor.query('SELECT * FROM people LIMIT 5')
        self.assertEqual((5, 3), data_via_sql_alchemy.shape)


class TestAPIExtract(unittest.TestCase):
    '''
    Tests the APIExtract class methods.
    User must update [path_to_reddit_config_file] and [path_to_openweather_config_file] to match their system's
    absolute path to the cloned repository.
    Users must also provide their own credentials to test functionality of the APIExtract class.
        -- Sign up for openweather API credentials: https://openweathermap.org/api
        -- Sign up for reddit API credentials for use with PRAW (Python Reddit API Wrapper): https://praw.readthedocs.io/en/stable/getting_started/quick_start.html
    '''
    path_to_reddit_config_file = '/home/smith/Development/ds5010/Secure/reddit_credentials.json' # user must update
    path_to_openweather_config_file = '/home/smith/Development/ds5010/Secure/open_weather_credentials.json' # user must update

    def test_init(self):
        '''
        Test the initialization of the TestAPIExtract class.
        Ensures the expected keys are present from the config_file used in initialization.
        '''
        api_extractor = APIExtract(config=self.path_to_reddit_config_file)
        self.assertEqual(['client_id', 'client_secret'], list(api_extractor.config.keys()))

    def test_load_config(self):
        '''
        Test the loading of a config file.
        Ensures the expected keys are present from the input config file.
        '''
        api_extractor = APIExtract(config=self.path_to_reddit_config_file)
        self.assertIsInstance(api_extractor.load_config(self.path_to_reddit_config_file), dict)

    def test_fetch_data(self):
        '''
        Test the fetch_data method of the TestAPIExtract class
        Ensures that, with a valid set of credentials, the data returned from the API endpoint contains the expected keys.
        '''
        api_extractor = APIExtract(config=self.path_to_openweather_config_file)
        params = {'q': 'Boston', 'units': 'imperial'}
        df = api_extractor.fetch_data(url='https://api.openweathermap.org/data/2.5', command='/weather', params=params)
        self.assertEqual(
            ['coord', 'weather', 'base', 'main', 'visibility', 'wind', 'clouds', 'dt', 'sys', 'timezone', 'id', 'name',
             'cod'], list(df.keys()))

    def test_get_top_n_reddit_posts(self):
        '''
        Test the get_top_n_reddit_posts method of the TestAPIExtract class
        Ensures that, with a valid set of credentials, the data returned from the API endpoint contains the expected number of rows and columns.
        '''
        api_extractor = APIExtract(config=self.path_to_reddit_config_file)
        top_10_reddit_posts = api_extractor.get_top_n_reddit_posts(sub='python')
        self.assertEqual((10, 4), top_10_reddit_posts.shape)


def main():
    unittest.main()  # invoke every method


if __name__ == '__main__':
    main()
