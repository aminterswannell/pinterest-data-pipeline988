import pandas as pd
import yaml
import tabula
from database_utils import DatabaseConnector as db_connector

class DataExtractor():
     
     def __init__(self, filename):
         self.filename = filename 
         
     def read_rds_table(self, db_connector):
         engine = db_connector.init_db_engine()
    
     def retrieve_pdf_data(self, pdf_link):
         df_pdf = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
         df_card_details = pd.concat(df_pdf, ignore_index=True)
         return df_card_details