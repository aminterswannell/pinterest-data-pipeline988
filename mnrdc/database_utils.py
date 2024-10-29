import yaml
from sqlalchemy import create_engine 
from sqlalchemy import inspect


class DatabaseConnector():
     
     def __init__(self, creds):
         self.creds = creds
         
     def read_db_creds(self, file_name):
         with open(file_name, 'r'):
             return yaml.safe_load(file_name)   

     def init_db_engine(self):  
         engine = create_engine(self.creds)
         return engine
     
     def list_db_tables(self):
         engine = self.init_db_engine()
         inspector = inspect(engine)
         return inspector.get_table_names()
     
     def upload_to_db(self, engine, df, table_name):
         df.to_sql(table_name, engine, if_exists = 'replace', index = False)
