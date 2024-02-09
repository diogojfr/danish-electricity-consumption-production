import os 
import sys
import pandas as pd
import requests 
import json
from src.exception import CustomException
from src.logger import logging


@dataclass

class DataExtractionConfig:
    data_path = str=os.path.join('artifacts','raw_tables','raw_data.csv')

class DataExtraction:
    def __init__(self):
        self.extraction_config = DataExtractionConfig()

    def initiate_data_extraction(self, dataset_name):

        logging.info('Entered the data extraction process...')
        
        try:
            url = "https://api.energidataservice.dk/dataset/{}?limit=48".format(dataset_name)

            response = requests.get(url)

            if response.status_code == 200:
                logging.info('Api successufully conected')


        except Exception as e:
            raise CustomException(e,sys)

        dataset_name = "ProductionConsumptionSettlement"




output = response.json()
data = output['records']

df = pd.DataFrame(data)