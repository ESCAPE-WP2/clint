import unittest
from clint import read_config_csv, read_storage_csv

class TestClint(unittest.TestCase):

    def test_read_config_csv(self):
        data = read_config_csv("tests/sample_config.csv")
        self.assertGreater(len(data), 0)
        self.assertIn('src', data[0])
        self.assertIn('dst', data[0])

    def test_read_storage_csv(self):
        data = read_storage_csv("tests/sample_storage.lst", True)
        self.assertGreater(len(data), 0)
        self.assertIn('did', data[0])
        self.assertIn('bytes', data[0])

if __name__ == "__main__":
    unittest.main()
