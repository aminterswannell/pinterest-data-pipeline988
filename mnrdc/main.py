from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import yaml
import psycopg2
import urllib.parse

source_dc = DatabaseConnector('db_creds.yaml')
dest_dc = DatabaseConnector('sales_data_db_creds.yaml')

source_de = DataExtractor('db_creds.yaml')
dest_de = DataExtractor('sales_data_db_creds.yaml')

source_creds = source_dc.read_db_creds('db_creds.yaml')
dest_creds = dest_dc.read_db_creds('sales_data_db_creds.yaml')

dest_password = urllib.parse.quote(dest_creds['PASSWORD'], safe='')

source_db_uri = f"postgresql://{source_creds['RDS_USER']}:{urllib.parse.quote(source_creds['RDS_PASSWORD'], safe='')}" \
                    f"@{source_creds['RDS_HOST']}:{source_creds['RDS_PORT']}/{source_creds['RDS_DATABASE']}"
dest_db_uri = f"postgresql://{dest_creds['USER']}:{dest_password}" \
                    f"@{dest_creds['HOST']}:{dest_creds['PORT']}/{dest_creds['DATABASE']}"

source_db_conn = DatabaseConnector(source_db_uri)
dest_db_conn = DatabaseConnector(dest_db_uri)

table_name = source_db_conn.list_db_tables()[0] 
user_data_df = source_de.read_rds_table(source_db_conn, table_name)

clean = DataCleaning()
clean_user_data_df = clean.clean_user_data(user_data_df)

engine = dest_db_conn.init_db_engine() 
dest_db_conn.upload_to_db(clean_user_data_df, 'dim_users', engine)