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


    def test_ice_pack_allotment(self):
        #data_dict = {
        #"temperature_min": [-10, 4, 10, 16, 19, 24, 30],
        #"temperature_max": [4, 10, 16, 19, 24, 30, 35],
        #"S": [1, 1, 2, 2, 3, 4, 5],
        #"M": [1, 2, 3, 3, 4, 5, 6],
        #"L": [1, 2, 3, 4, 5, 6, 7],
        #}

        #df_ice_packinfo = pd.DataFrame(data_dict)


        data_dict2 = {
            "PRODUCTION_DATE": [
                "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04",
                "2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08"
            ],
            "EXPECTED_DELIVERY_DATE": [
                "2024-01-02", "2024-01-04", "2024-01-05", "2024-01-07",
                "2024-01-06", "2024-01-08", "2024-01-10", "2024-01-10"
            ],
            "COOL_POUCH_SIZE": ["S", "M", "L", "XL", "S", "M", "L", "XL"],
            "AVERAGE_TEMPERATURE": [-2, 0, 5, 11, 15, 20, 22, 26],
            "NUMBER_OF_ICE_PACKS" : [None,None,None,None,None,None,None,None]
        }

        df_order_data = pd.DataFrame(data_dict2)

        processor = DataProcessor()
        self.df = processor.allocate_ice_packs_to_orders(dataframe=df_order_data)
        self.assertEqual(self.df.at[0, 'NUMBER_OF_ICE_PACKS'], 1)
        self.assertEqual(self.df.at[1, 'NUMBER_OF_ICE_PACKS'], 3)
        self.assertEqual(self.df.at[3, 'NUMBER_OF_ICE_PACKS'], 7)



    