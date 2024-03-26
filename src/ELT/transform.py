import os
import sys
from src.exception import CustomException
from dataclasses import dataclass


@dataclass

class DataTransforming:

    def run_dbt_transformations(self, dbt_project_name):
        
        #try:
            path_dir = os.getcwd()
            path_dbt = os.path.join(path_dir,dbt_project_name)
            os.chdir(path_dbt)
            #os.system('conda activate venv/')
            os.system('dbt run')
            
        # except Exception as e:
        #     raise CustomException(sys, e)