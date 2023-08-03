from unittest import TestCase
from pyspark.sql import SparkSession


import sys 
sys.path.insert(1, '/Users/rajsingh/work/PYSPARK/pyspark-repo/')
from modules.source import Source
from modules.transformation import Transformation

class UtilsTestCase(TestCase):
    @classmethod
    def setUpClass(cls)-> None:
        cls.spark = SparkSession.builder \
                .master("local[3]") \
                .appName("HelloSparkTest") \
                .getOrCreate()
        
    
    def test_datafile_loading(self):
        source = Source()
        sample_df = source.read_csv(self.spark, "../data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count,9, "Record Count shoe be 9")
    
    def test_country_count(self):
        source = Source()
        transformation = Transformation()
        sample_df = source.read_csv(self.spark, "../data/sample.csv")
        count_list = transformation.count_by_country(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row['Country']] = row['count']

        print(count_dict)
        
        self.assertEqual(count_dict['United States'],4,"Count for United States should be 4")
        self.assertEqual(count_dict['Canada'],2,"Count for Canada should be 2")
        self.assertEqual(count_dict['United Kingdom'],1,"Count for United Kingdom should be 1")
