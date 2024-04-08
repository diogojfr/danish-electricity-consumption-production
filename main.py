from src.ELT.extract import DataExtraction
from src.ELT.load import DataLoading
from src.ELT.transform import DataTransforming

# api config
dataset_name = "ProductionConsumptionSettlement"
#number_of_days = 365
year_start = 2022
year_end = 2024

# postgres config
hostname = 'localhost'
database = 'danish_energy'
username = 'postgres'
pwd = 'password'
port_id = 5432

# dbt tranform config
dbt_project_name = 'danish_energy_consumption'

if __name__ == "__main__":
    data_extract = DataExtraction()
    data_path = data_extract.initiate_data_extraction(dataset_name, year_start, year_end)

    data_loading = DataLoading()
    data_loading.initiate_data_loading(data_path,hostname,database,username,pwd,port_id)
    
    data_transform = DataTransforming()
    data_transform.run_dbt_transformations(dbt_project_name)