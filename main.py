import os
import sys
import pandas as pd
from src.components.extract import DataExtraction

dataset_name = "ProductionConsumptionSettlement"
number_of_days = 1

if __name__ == "__main__":
    data_extract = DataExtraction()
    data_path = data_extract.initiate_data_extraction(dataset_name, number_of_days)
    df = pd.read_csv(data_path)