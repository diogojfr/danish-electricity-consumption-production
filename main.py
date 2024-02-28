import os
import sys
import pandas as pd
from src.ETL.extract import DataExtraction
from src.ETL.load import DataLoading

dataset_name = "ProductionConsumptionSettlement"
number_of_days = 2
database_user = "user"
database_password = "password"

if __name__ == "__main__":
    data_extract = DataExtraction()
    data_path = data_extract.initiate_data_extraction(dataset_name, number_of_days)
    #df = pd.read_csv(data_path)

    data_loading = DataLoading()
    data_loading.initiate_data_loading(data_path,database_user,database_password)
    