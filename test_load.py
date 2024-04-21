'''
Smith Elie
DS5010: test_extract
A suite of tests for testing the load module.
'''

import unittest
import pandas as pd
from extract import FileExtract
from transform import Transformer
from load import Loader


class TestLoader(unittest.TestCase):
    path_to_tabular_data_with_array = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/tabular_with_array_column.csv'
    output_path_for_loader = '/home/smith/Development/ds5010/github/ds5010/etl_tool/data/outbox/data_written_by_file_loader.csv'

    def test_init(self):
        # pass some raw data to an instance of the Transformer class for testing
        file_extractor = FileExtract()
        raw_tabular_data = file_extractor.read_tabular(file_path=self.path_to_tabular_data_with_array, delimiter=',')
        transformer = Transformer(raw_data=raw_tabular_data)
        retrieved_data = transformer.get_data()
        file_loader = Loader(data=retrieved_data)
        self.assertEqual((4, 2), file_loader.data.shape)

    def test_write_to_file(self):
        file_extractor = FileExtract()
        raw_tabular_data = file_extractor.read_tabular(file_path=self.path_to_tabular_data_with_array, delimiter=',')
        file_loader = Loader(data=raw_tabular_data)
        file_loader.write_to_file(path=self.output_path_for_loader)


def main():
    unittest.main()  # invoke every method


if __name__ == '__main__':
    main()
