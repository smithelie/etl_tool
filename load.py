from extract import FileExtract, DatabaseExtract, APIExtract
from transform import Transformer
import pandas as pd
import boto3


class Loader():
    '''
    The Loader class provides functionality to write data to either local filesystems or AWS S3 buckets.
    It supports custom delimiters for the output files.
    Methods: write_to_file, write_to_s3
    '''

    def __init__(self, data: pd.DataFrame):
        '''
        Initializes the Loader object with a pandas dataframe.
        :param data: The pandas data frame to be written to a file or S3 bucket.
        '''
        if isinstance(data, pd.DataFrame):
            self.data = data
        else:
            raise ValueError('Only dataframes can be used in the constructor.')

    def write_to_file(self, path: str, delimiter=',', index=False, **kwargs):
        '''
        Writes the pandas data frame to a local file in CSV format with a specified delimiter.
        :param path: The file path or buffer where the CSV file will be saved.
        :param delimiter: The delimiter character to use in the CSV file (default is ',').
        :param index: Whether to write row names (index). Default is False.
        :param kwargs: Any additional keyword arguments to pass to pd.DataFrame.to_csv().
        '''
        try:
            self.data.to_csv(path_or_buf=path, sep=delimiter, index=index, **kwargs)
        except Exception as e:
            print(f'Something went wrong writing the data. Here are the details.\n{e}')

    def write_to_s3(self, bucket_name: str, output_file_name: str, delimiter=',', **kwargs):
        '''
        Writes the pandas data frame to a CSV file and uploads it to an AWS S3 bucket using a specified delimiter.
        :param bucket_name: The name of the AWS S3 bucket.
        :param output_file_name: The name of the file to create within the S3 bucket.
        :param delimiter: The delimiter character to use in the CSV file (default is ',').
        :param kwargs: Any additional keyword arguments to pass to pd.DataFrame.to_csv().
        '''
        s3_path = f's3://{bucket_name}/{output_file_name}'  # construct full s3 URI from bucket and object key
        try:
            self.data.to_csv(path_or_buf=s3_path, sep=delimiter, index=False, **kwargs)
            print(f'Successfully wrote results to {s3_path}.')
        except Exception as e:
            print(f'Something went wrong writing the data to s3. Here are the details.\n{e}')
