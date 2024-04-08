import os 
import sys
import pandas as pd
import requests 
import json
from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging


@dataclass

class DataExtractionConfig:
    data_path = str=os.path.join('artifacts','raw_tables','raw_data.csv')

class DataExtraction:
    def __init__(self):
        self.extraction_config = DataExtractionConfig()

    def initiate_data_extraction(self, dataset_name, year_start, year_end):

        logging.info('Entered the data extraction process...')
        
        try:
            #url = "https://api.energidataservice.dk/dataset/{}?limit={}".format(dataset_name,number_days*48)
            url = "https://api.energidataservice.dk/dataset/{}?start={}-01-01T00:00&end={}-01-01T00:00".format(dataset_name,year_start, year_end+1)

            response = requests.get(url)

            if response.status_code == 200:
                logging.info('API successfully connected.')
            
            output = response.json()
            data = output['records']

            df = pd.DataFrame(data)

            df.to_csv(self.extraction_config.data_path, index=False, header=True)

            logging.info('Data extraction completed.')

            return self.extraction_config.data_path

        except Exception as e:
            raise CustomException(e,sys)






