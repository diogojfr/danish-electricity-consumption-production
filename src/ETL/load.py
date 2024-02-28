import os
import sys
import pandas as pd
import numpy as np
from src.logger import logging
from src.exception import CustomException
from utils import create_table
from utils import loading_data
from dataclasses import dataclass

@dataclass

# class DataLoadingConfig:
#     df_filtered_path = os.path.join('artifacts','raw_tables','df_filtered.csv')

class DataLoading:
    #def __init__(self, user, password):
    #    self.loading_config = DataLoadingConfig()

    def initiate_data_loading(self, extraction_path, user, password):
        logging.info('Entered data loading process...')
        
        try:
            df = pd.read_csv(extraction_path)

            logging.info('Filtering the dataset...')
            df = df[df['PriceArea']=='DK1']
            df = df.reset_index(drop=True)
            logging.info('Dataset filtering completed.')

            logging.info('Changing the columns types...')
            #df['HourDK'] = pd.to_datetime(df['HourDK'], errors='coerce')
            df['HourUTC'] = pd.to_datetime(df['HourUTC'], errors='coerce')
            logging.info('Columns types changes completed.')

            logging.info('Loading the dataset into the database (elephantsql)...')
            create_table(user, password)
            loading_data(df, user, password)
            logging.info('The dataset has been loaded into the database.')

        except Exception as e:
            raise CustomException(sys, e)

         

#df = pd.read_csv("C:/Users/Administrador/Documents/data-science/portifolio-projects/danish-electricity-consumption-and-production/artifacts/raw_tables/raw_data.csv")

