'''
Smith Elie
DS5010: test_extract
A suite of tests for testing the transform module class.
TestTransformer:
    - User must update [path_to_tabular_data_with_array] and [path_to_json_data] to match their system's absolute path to the cloned repository.
'''

import unittest
import pandas as pd
from extract import FileExtract
from transform import Transformer


class TestTransformer(unittest.TestCase):
    path_to_tabular_data_with_array = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/tabular_with_array_column.csv' # user must update
    path_to_json_data = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/json.json' # user must update

    def test_init(self):
        # pass some raw data to an instance of the Transformer class for testing
        file_extractor = FileExtract()
        raw_tabular_data = file_extractor.read_tabular(file_path=self.path_to_tabular_data_with_array, delimiter=',')
        transformer = Transformer(raw_data=raw_tabular_data)
        self.assertEqual((4, 2), transformer.raw_data.shape)  # verify assignment worked properly

    def test_json_to_dataframe(self):
        file_extractor = FileExtract()
        raw_json_data = file_extractor.read_json(file_path=self.path_to_json_data)
        transformer = Transformer(raw_data=raw_json_data)
        processed_data = transformer.json_to_dataframe().get_data()
        self.assertIsInstance(processed_data, pd.DataFrame)

    def test_replace_values(self):
        file_extractor = FileExtract()
        raw_tabular_data = file_extractor.read_tabular(file_path=self.path_to_tabular_data_with_array, delimiter=',')
        transformer = Transformer(raw_data=raw_tabular_data)
        transformer.replace_values('name', 'john', 'replaced with adam')
        processed_data = transformer.get_data()
        self.assertEqual('replaced with adam', processed_data.loc[0, 'name'])

    def test_fill_missing_values(self):
        file_extractor = FileExtract()
        raw_tabular_data = file_extractor.read_tabular(file_path=self.path_to_tabular_data_with_array, delimiter=',')
        transformer = Transformer(raw_data=raw_tabular_data)
        transformer.fill_missing_values(col='phone_numbers', fill_value='Was Missing')
        processed_data = transformer.get_data()
        self.assertEqual('Was Missing', processed_data.loc[2, 'phone_numbers'])

    def test_explode_column(self):
        file_extractor = FileExtract()
        raw_tabular_data = file_extractor.read_tabular(file_path=self.path_to_tabular_data_with_array, delimiter=',')
        transformer = Transformer(raw_data=raw_tabular_data)
        transformer.explode_column(col='phone_numbers', delimiter='|')
        processed_data = transformer.get_data()
        self.assertEqual((7, 2), processed_data.shape)

    def test_get_data(self):
        file_extractor = FileExtract()
        raw_tabular_data = file_extractor.read_tabular(file_path=self.path_to_tabular_data_with_array, delimiter=',')
        transformer = Transformer(raw_data=raw_tabular_data)
        retrieved_data = transformer.get_data()
        self.assertEqual((4, 2), retrieved_data.shape)


def main():
    unittest.main()  # invoke every method


if __name__ == '__main__':
    main()
