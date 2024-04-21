import pandas as pd
import json
from sqlalchemy import create_engine
import pyodbc
import praw
import requests


class FileExtract():
    '''
    The FileExtract class provides an object with methods for extracting data from various file formats. Instances of the
    class support the extraction of tabular and JSON formatted data. The read methods are implicit getters.
    Methods: read_tabular, read_json
    '''

    def __init__(self, file_path: str = None) -> None:
        '''
        Initializes the FileExtract object with an optional file path.
        :param file_path: Path to the file whose contents will be extracted. If one is not provided, a file path
                          must be provided with the read_tabular or read_json method calls.
        '''
        self.file_path = file_path

    def read_tabular(self, delimiter, file_path=None, **kwargs) -> pd.DataFrame:
        '''
        read_tabular: Reads tabular data, using a specified delimiter, and returns its contents as a pandas dataframe.
        :param delimiter: The delimiter used in the tabular data file.
        :param file_path: Path to the file whose contents will be extracted. If not provided, the file path specified
                          during object creation is used, otherwise, a value error is raised.
        :param kwargs: Any additional arguments supported by pd.read_csv are supported.
        :return: The extracted data as a pandas dataframe.
        '''
        effective_path = file_path if file_path is not None else self.file_path
        if effective_path is None:
            raise ValueError('File path must be set at initialization or provided.')
        if delimiter is None:
            raise ValueError('Delimiter must be provided.')
        try:
            return pd.read_csv(effective_path, sep=delimiter, **kwargs)
        except Exception as e:
            print(f'Something went wrong with reading the file. Here are the details.\n{e}')
            return pd.DataFrame()  # returns empty DataFrame on error.

    def read_json(self, file_path: str = None, **kwargs) -> dict:
        '''
        Reads a JSON formatted file and returns its contents as a dictionary.
        :param file_path: Path to the JSON file whose contents will be extracted. If not provided, the file path
                          specified during object creation is used, otherwise, a value error is raised.
        :param kwargs: Any additional arguments supported by json.load are supported.
        :return: The extracted data as a dictionary.
        '''
        effective_path = file_path if file_path is not None else self.file_path
        if effective_path is None:
            raise ValueError('File path must be set at initialization or provided.')
        try:
            with open(effective_path, 'r') as file:
                return json.load(file, **kwargs)
        except Exception as e:
            print(f'Something went wrong with reading the JSON file. Here are the details:\n{e}')
            return dict()  # returns empty dictionary on error.


class DatabaseExtract():
    '''
     The DatabaseExtract class provides an object with methods for querying data from various databases. Instances of the
     class support connections using SQLAlchemy or pyodbc depending on the input provided. Database-specific drivers must
     be installed for use with pyodbc. The query methods are implicit getters.
     Methods: query
     '''

    def __init__(self, connection_url: str = None, connection_str: str = None) -> None:
        '''
        Initializes the DatabaseExtract object with a connection string or URL.
        :param connection_url: A connection url to connect to a database with using SQLAlchemy.
        :param connection_str: A connection string to connect to a database with using pyodbc.
        '''
        if connection_url and connection_str:
            raise ValueError('Provide a connection_url or a connection_str, not both.')
        if connection_url:
            self.engine = create_engine(connection_url)
            self.connection = self.engine.connect()
            self.connection_type = 'sqlalchemy'
        elif connection_str:
            self.conn = pyodbc.connect(connection_str)
            self.connection_type = 'pyodbc'
        else:
            raise ValueError('A connection_url or connection_str must be provided.')

    def query(self, sql: str) -> pd.DataFrame:
        '''
        Executes a SQL query using the established database connection.
        :param sql: The SQL query to execute.
        :return: A pandas dataframe containing the results of the SQL query.
        '''
        if self.connection_type == 'sqlalchemy':
            try:
                return pd.read_sql_query(sql, self.connection)
            except Exception as e:
                print(f"Something went wrong with the SQLAlchemy query. Here are the details:\n{e}")
                return pd.DataFrame()  # Return empty pandas data frame on error
        elif self.connection_type == 'pyodbc':
            try:
                return pd.read_sql_query(sql, self.conn)
            except Exception as e:
                print(f"Something went wrong with the pyodbc query. Here are the details:\n{e}")
                return pd.DataFrame()  # Return empty pandas data frame on error


class APIExtract():
    '''
     The APIExtract class provides an object with methods for extracting data from APIs. Credentials can be passed in
     via a configuration file at initialization or when calling the fetch_data method. The class is flexible enough to
     allow credentials to be passed in through request headers or URL parameters to accommodate various API endpoint
     authentication patterns. The fetch and get methods are implicit getters.
     Methods: load_config, fetch_data, get_top_n_reddit_posts
    '''

    def __init__(self, config: str) -> None:
        '''
        Initializes the APIExtract object with a configuration file.
        :param config: The path to a configuration file.
        '''
        self.config = self.load_config(config)

    def load_config(self, path: str) -> dict:
        '''
        Loads a (JSON) configuration file into memory.
        :param path: The file path to the configuration file.
        :return: A dictionary containing configuration information (e.g. - parameters, request headers)
        '''
        with open(path, 'r') as file:
            return json.load(file)

    def fetch_data(self, url: str, command: str = '', params: dict = None, headers: dict = None) -> dict | list:
        '''
        Sends a GET request to the specified API URL for data. Optionally appends a command/endpoint to the URL.
        The method integrates configurations from the configuration file used at object initialization with any
        parameters or headers passed in at method call. Configurations in the config file can be overwritten at method
        call.
        :param url: The base URL for the API endpoint.
        :param command: Command/endpoint to append to the base URL.
        :param params: Query parameters to include with the GET request.
        :param headers: HTTP headers to send with the GET request.
        :return: The response as a dictionary or list of dictionaries, depending on the API provider.
        '''
        # use values from config else init empty dictionaries
        _params = self.config.get('parameters', {}).copy()
        _headers = self.config.get('headers', {}).copy()
        # if supplying parameters at method call, update params dictionary with new parameters
        if params:
            _params.update(params)
        # if supplying headers at method call, update headers dictionary with new headers
        if headers:
            _headers.update(headers)
        url += command  # append command/endpoint to base URL
        try:
            response = requests.get(url, params=_params, headers=_headers)
            return response.json()
        except Exception as e:
            print(f'Something went wrong with the API call. Here are the details.\n{e}')
            return dict()  # Return empty dictionary if error

    def get_top_n_reddit_posts(self, sub: str, user_agent: str = 'localhost', top: int = 10) -> pd.DataFrame:
        '''
        Retrieves the top N posts from a specified subreddit.
        :param sub: The subreddit from which to retrieve posts.
        :param user_agent: The user agent to use for the Reddit API. Defaults to 'localhost'.
        :param top: The number of posts to retrieve. Defaults to 10.
        :return: A pandas dataframe containing the top posts with columns for title, score, url and creation time.
        '''
        # pull credentials from Reddit API with praw-specific configuration file. Praw is a module that handles
        # authC/authZ to the Reddit API.
        client_id, client_secret = self.config['client_id'], self.config['client_secret']
        try:
            reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
            subreddit = reddit.subreddit(sub)
            top_posts = subreddit.top(limit=top)
            # pull relevant fields from each posts in posts captured in request
            data = [{"title": post.title, "score": post.score, "url": post.url, "created": post.created_utc}
                    for post in top_posts]
            return pd.DataFrame(data)
        except Exception as e:
            print(f'Something went wrong with getting data from the reddit API. Here are the details\n{e}')
            return pd.DataFrame()  # Return empty pandas data frame if error.
