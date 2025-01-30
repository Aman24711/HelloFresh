import unittest

from DataProcessor import DataProcessor

class DataProcessorsTests(unittest.TestCase):
    def test_data_processors(self):
        processor = DataProcessor()
        a = processor.get_outcode('BT57EW')
        c = processor.get_outcode('BT57 EW')
        self.assertEqual(a, 'BT5')
        self.assertEqual(c, 'BT5')

    def test_data_outcode_error(self):
        processor = DataProcessor()
        self.assertRaises(ValueError, processor.get_outcode, 'EW')

    