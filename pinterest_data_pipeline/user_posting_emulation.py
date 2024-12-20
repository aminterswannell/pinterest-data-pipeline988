import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml 
from datetime import datetime

random.seed(100)


class AWSDBConnector:

    def __init__(self, creds):
        self.creds = creds

    def read_db_creds(self):
        with open(self.creds, 'r') as stream:
           db_creds = yaml.safe_load(stream)
        return db_creds
    
    def create_db_connector(self):
        connection_creds = self.read_db_creds()
        self.HOST = connection_creds['HOST']
        self.USER = connection_creds['USER']
        self.PASSWORD = connection_creds['PASSWORD']
        self.DATABASE = connection_creds['DATABASE']
        self.PORT = connection_creds['PORT']
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector('db_creds.yaml')


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)
            
            pin_invoke_url = "https://szsizzrj2m.execute-api.us-east-1.amazonaws.com/dev/topics/129fb6ae3c55.pin"
            geo_invoke_url = "https://szsizzrj2m.execute-api.us-east-1.amazonaws.com/dev/topics/129fb6ae3c55.geo"
            user_invoke_url = "https://szsizzrj2m.execute-api.us-east-1.amazonaws.com/dev/topics/129fb6ae3c55.user"

            pin_payload = json.dumps({
                "records": [
                    {
                        "value" : {"index" : pin_result["index"],
                                   "unique_id" : pin_result["unique_id"],
                                   "title" : pin_result["title"],
                                   "description" : pin_result["description"],
                                   "poster_name" : pin_result["poster_name"],
                                   "follower_count" : pin_result["follower_count"],
                                   "tag_list" : pin_result["tag_list"],
                                   "is_image_or_video" : pin_result["is_image_or_video"],
                                   "image_src" : pin_result["image_src"],
                                   "downloaded" : pin_result["downloaded"],
                                   "save_location" : pin_result["save_location"],
                                   "category" : pin_result["category"]}
                    }
                ]
            })

            geo_payload = json.dumps({
                "records" : [
                    {
                        "value" : {"ind" : geo_result["ind"],
                                   "timestamp" : geo_result["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                                   "latitude" : geo_result["latitude"],
                                   "longitude" : geo_result["longitude"],
                                   "country" : geo_result["country"]}
                    }
                ]
            })

            user_payload = json.dumps({
                "records" : [
                    {
                        "value" : {"ind" : user_result["ind"],
                                   "first_name" : user_result["first_name"],
                                   "last_name" : user_result["last_name"],
                                   "age" : user_result["age"],
                                   "date_joined" : user_result["date_joined"].strftime("%Y-%m-%d %H:%M:%S")}
                    }
                ]
            })
            
            header = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

            pin_response = requests.request("POST", pin_invoke_url, headers=header, data= pin_payload)
            geo_response = requests.request("POST", geo_invoke_url, headers=header, data= geo_payload)
            user_response = requests.request("POST", user_invoke_url, headers=header, data= user_payload)

            print(f"pin: {pin_response.status_code}")
            print(f"geo: {geo_response.status_code}")
            print(f"user: {user_response.status_code}")

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')

