'''
Smith Elie
DS5010: test_extract
A suite of tests for testing the extract module.
Sign up for openweather API credentials: https://openweathermap.org/api
Sign up for reddit API credentials for use with PRAW (Python Reddit API Wrapper): https://praw.readthedocs.io/en/stable/getting_started/quick_start.html
'''

import unittest
from extract import FileExtract, DatabaseExtract, APIExtract


class TestFileExtract(unittest.TestCase):
    path_to_tabular_data = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/tabular.csv'
    path_to_json_data = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/json.json'

    def test_init_with_file_path(self):
        file_extractor = FileExtract(self.path_to_tabular_data)
        self.assertEqual(self.path_to_tabular_data, file_extractor.file_path)

    def test_read_tabular(self):
        file_extractor = FileExtract()
        tabular_data = file_extractor.read_tabular(delimiter=',', file_path=self.path_to_tabular_data)
        self.assertEqual((32, 12), tabular_data.shape)  # expected vs actual

    def test_read_json(self):
        file_extractor = FileExtract()
        json_data = file_extractor.read_json(file_path=self.path_to_json_data)
        self.assertEqual(['id', 'name', 'email', 'address', 'orders'], list(json_data.keys()))  # expected vs actual


class TestDatabaseExtract(unittest.TestCase):
    connection_str = 'DRIVER=SQLite3;DATABASE=data/database.db'  # user supplied
    connection_url = 'sqlite://///home/smith/Development/ds5010/github/ds5010/etl_tool/data/database.db'  # user supplied

    def test_init_with_conn_str(self):
        database_extractor = DatabaseExtract(connection_str=self.connection_str)  # user supplied
        self.assertEqual('pyodbc', database_extractor.connection_type)  # expected vs actual. Assignment test.

    def test_init_with_conn_url(self):
        database_extractor = DatabaseExtract(connection_url=self.connection_url)  # user supplied
        self.assertEqual('sqlalchemy', database_extractor.connection_type)  # expected vs actual. Assignment test.

    def test_query_with_conn_str(self):
        database_extractor = DatabaseExtract(connection_str=self.connection_str)  # user supplied
        data_via_pyodbc = database_extractor.query('SELECT * FROM people LIMIT 3')
        self.assertEqual((3, 3), data_via_pyodbc.shape)

    def test_query_with_conn_url(self):
        database_extractor = DatabaseExtract(connection_url=self.connection_url)  # user supplied
        data_via_sql_alchemy = database_extractor.query('SELECT * FROM people LIMIT 5')
        self.assertEqual((5, 3), data_via_sql_alchemy.shape)


class TestAPIExtract(unittest.TestCase):
    path_to_reddit_config_file = '/home/smith/Development/ds5010/Secure/reddit_credentials.json'
    path_to_openweather_config_file = '/home/smith/Development/ds5010/Secure/open_weather_credentials.json'

    def test_init(self):
        api_extractor = APIExtract(config=self.path_to_reddit_config_file)  # user supplied
        self.assertEqual(['client_id', 'client_secret'], list(api_extractor.config.keys()))

    def test_load_config(self):
        api_extractor = APIExtract(config=self.path_to_reddit_config_file)  # user supplied
        self.assertIsInstance(api_extractor.load_config(self.path_to_reddit_config_file), dict)  # user supplied

    def test_fetch_data(self):
        api_extractor = APIExtract(config=self.path_to_openweather_config_file)
        params = {'q': 'Boston', 'units': 'imperial'}
        df = api_extractor.fetch_data(url='https://api.openweathermap.org/data/2.5', command='/weather', params=params)
        self.assertEqual(
            ['coord', 'weather', 'base', 'main', 'visibility', 'wind', 'clouds', 'dt', 'sys', 'timezone', 'id', 'name',
             'cod'], list(df.keys()))

    def test_get_top_n_reddit_posts(self):
        api_extractor = APIExtract(config=self.path_to_reddit_config_file)  # user supplied
        top_10_reddit_posts = api_extractor.get_top_n_reddit_posts(sub='python')
        self.assertEqual((10, 4), top_10_reddit_posts.shape)


def main():
    unittest.main()  # invoke every method


if __name__ == '__main__':
    main()
