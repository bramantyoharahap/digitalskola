import requests
import pandas as pd
import logging

class CovidScraper():

    def __init__(self, url):
        self.url = url

    def fetch_data(self):
        response = requests.get(self.url)
        result = response.json()['data']['content']
        logging.info('GET DATA FROM API COMPLETED')
        df = pd.json_normalize(result)
        logging.info('CONVERT DATA FROM API TO DATAFRAME COMPLETED')
        return df
