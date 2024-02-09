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

    def initiate_data_extraction(self, dataset_name, number_days):

        logging.info('Entered the data extraction process...')
        
        try:
            url = "https://api.energidataservice.dk/dataset/{}?limit={}".format(dataset_name,number_days*48)

            response = requests.get(url)

            if response.status_code == 200:
                logging.info('Api successufully connected')
            
            output = response.json()
            data = output['records']

            df = pd.DataFrame(data)

            df.to_csv(self.extraction_config.data_path, index=False, header=True)

            logging.info('Data extraction completed.')

            return self.extraction_config.data_path

        except Exception as e:
            raise CustomException(e,sys)






