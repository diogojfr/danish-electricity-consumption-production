import os
import sys
import pandas as pd
from src.ELT.extract import DataExtraction
from src.ELT.load import DataLoading

# api config
dataset_name = "ProductionConsumptionSettlement"
number_of_days = 2

# postgres config
hostname = 'localhost'
database = 'danish_energy'
username = 'postgres'
pwd = 'password'
port_id = 5432

if __name__ == "__main__":
    data_extract = DataExtraction()
    data_path = data_extract.initiate_data_extraction(dataset_name, number_of_days)

    data_loading = DataLoading()
    data_loading.initiate_data_loading(data_path,hostname,database,username,pwd,port_id)
    
    