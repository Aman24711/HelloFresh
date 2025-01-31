import unittest
import pandas as pd
from DataProcessor import DataProcessor

class DataProcessorsTests(unittest.TestCase):

    def setUp(self):
        columns = ['LATITUDE', 'LONGITUDE', 'OUTCODE']
        data = [(None, None, "GY4"), (None, None, "BT71"), (None, None, "GY8")]
        self.df = pd.DataFrame(data, columns=columns)

    


    def test_data_processors(self):
        processor = DataProcessor()
        a = processor.get_outcode('BT57EW')
        c = processor.get_outcode('BT57 EW')
        self.assertEqual(a, 'BT5')
        self.assertEqual(c, 'BT5')

    def test_data_outcode_error(self):
        processor = DataProcessor()
        self.assertRaises(ValueError, processor.get_outcode, 'EW')


    def test_inserting_latlong(self):
        processor = DataProcessor()
        self.df = processor.insert_latlong(self.df)
        self.assertEqual(self.df.at[0, 'LATITUDE'], 49.4336)
        self.assertEqual(self.df.at[0, 'LONGITUDE'], -2.5594)

    
    def test_inserting_temperature_nans(self):
        columns = ['LATITUDE', 'LONGITUDE', 'AVERAGE_TEMPERATURE','EXPECTED_DELIVERY_DATE']
        data = [(49.4336, -2.5594, None, '2025-01-28'), (49.4336, -2.5594, 5.0, '2025-01-29'), (48.4336, -3.5594, 3.0, '2025-01-28'), (48.9336, -3.7594, 4.0, '2025-01-28')]
        dataframe = pd.DataFrame(data, columns=columns)

        processor = DataProcessor()
        self.df = processor.fill_dataframe_temperature_nan(dataframe=dataframe)
        self.assertEqual(self.df.at[0, 'AVERAGE_TEMPERATURE'], 4.0)

    