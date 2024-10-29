import pandas as pd
import numpy as np
from data_extraction import DataExtractor as de

class DataCleaning():
     
     def __init__(self):
         pass
     
     def clean_user_data(self, df):
         df = self.de.read_rds_table('')
         df.dropna(inplace = True)
         df['Date'] = pd.to_datetime(df['Date'])
         return df