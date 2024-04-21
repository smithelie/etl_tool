from extract import FileExtract, DatabaseExtract, APIExtract
import pandas as pd


class Transformer():
    '''
    The Transformer class provides an object for applying common transformations to data. It is intended to mutate the
    data directly to keep the memory footprint light. This class can be expanded with more transformations through the
    addition of more methods.
    Methods: json_to_dataframe, replace_values, fill_missing_values, explode_column, get_data
    '''

    def __init__(self, raw_data):
        '''
        Initializes the Transformer object with raw data.
        :param raw_data: The raw dataset to be transformed.
        '''
        self.raw_data = raw_data

    def json_to_dataframe(self, max_level: int = None, **kwargs):
        '''
        Transforms a JSON-like structure into a flattened pandas DataFrame.
        :param max_level: The maximum level of flattening. If None, flattens completely.
        :param kwargs: Any additional arguments supported by pd.json_normalize are supported.
        :return: Returns instance of the Transformer object with updated attributes. This makes method chaining possible.
        '''
        if max_level is not None:
            # update the max_level arg passed to json_normalize with whatever user passes to json_to_dataframe
            kwargs['max_level'] = max_level
        self.raw_data = pd.json_normalize(data=self.raw_data, **kwargs)
        return self

    def replace_values(self, col: str, to_replace, value, **kwargs):
        '''
        Replaces occurrences of `to_replace` in the specified column with `value`.
        :param col: Column in the DataFrame where replacements need to be made.
        :param to_replace: Values to find and replace.
        :param value: Value to replace with.
        :param kwargs: Any additional keyword arguments supported by pd.Series.replace are supported.
        :return: Returns instance of the Transformer object with updated attributes. This makes method chaining possible.
        '''
        if col not in self.raw_data.columns:
            raise ValueError(f"The column {col} does not exist in the DataFrame.")
        self.raw_data[col] = self.raw_data[col].replace(to_replace, value, **kwargs)
        return self

    def fill_missing_values(self, col: str, fill_value='', **kwargs):
        '''
        Fills missing/null values in the specified column with a defined value.
        :param col: Column in the DataFrame to fill missing values in.
        :param fill_value: Value used to fill missing values.
        :param kwargs: Any additional keyword arguments supported by pd.Series.fillna are supported.
        :return: Returns instance of the Transformer object with updated attributes. This makes method chaining possible.
        '''
        if col not in self.raw_data.columns:
            raise ValueError(f"The column {col} does not exist in the DataFrame.")
        self.raw_data[col] = self.raw_data[col].fillna(fill_value, **kwargs)
        return self

    def explode_column(self, col: str, delimiter: str = ','):
        '''
        Expands the elements in a column split by a delimiter into rows.
        :param col: The column whose string content is split and expanded into separate rows.
        :param delimiter: The delimiter used to split the column strings.
        :return: Returns instance of the Transformer object with updated attributes. This makes method chaining possible.
        '''
        if col not in self.raw_data.columns:
            raise ValueError(f'The column {col} does not exist in the DataFrame.')
        self.raw_data[col] = self.raw_data[col].str.split(delimiter)
        self.raw_data = self.raw_data.explode(col)
        return self

    def get_data(self):
        '''
        Returns the current state of the data in the Transformer object. Useful for further analysis or processing
        outside the class.
        :return: A pandas DataFrame containing the transformed data.
        '''
        return self.raw_data
