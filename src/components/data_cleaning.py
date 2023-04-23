import os
import sys
import pandas as pd
from src.logger import logging
from src.exception import CustomException
from src.utils import Haversine_np

## Intitialize the Data Cleaning Configuration

@dataclass
class DataCleaningconfig:

    Cleaned_data_path:str=os.path.join('artifacts','Cleaned_data.csv')


class DataCleaning:

    def __init__(self):
        self.cleaning_config = DataCleaningconfig()


    def initiate_data_cleaning(self):
        
        logging.config('Data Cleaning starts')

        try:
            pass
            df=pd.read_csv(os.path.join('notebooks/data','finalTrain.csv'))
            logging.info('Dataset read as pandas Dataframe for cleaning')

            os.makedirs(os.path.dirname(self.cleaning_config.Cleaned_data_path),exist_ok=True)
            
            df = df[~df.isna().any(axis=1)]
            
            df = df[~((~df.Time_Orderd.str.contains(r'[0-9]{2}:[0-9]{2}',na=False)) | (~df.Time_Order_picked.str.contains(r'[0-9]{2}:[0-9]{2}',na=False)))]

            df['Time_Orderd'] = pd.to_datetime(df['Order_Date'] + ' ' + df['Time_Orderd'])

            df.Time_Order_picked = df.Time_Order_picked.str.replace('24','00')

            df['Time_Order_picked'] = pd.to_datetime(df['Order_Date'] + ' ' + df['Time_Order_picked'])

            diff = (pd.to_datetime(d2['Time_Order_picked']) - pd.to_datetime(d2['Time_Orderd'])).dt.seconds/60 
            df.insert(loc=11, column='Prep_time', value=diff)

            distance_km = haversine_np(df['Restaurant_longitude'],df['Restaurant_latitude'],df['Delivery_location_longitude'],df['Delivery_location_latitude'])
            df.insert(loc=20,column='Distance',value=distance_km.round(2))

            df.columns = df.columns.str.replace(' ','').str.replace("(",'_').str.replace(")",'')

            loggin.info("Data Cleaning completed")

            df.to_csv(self.cleaning_config.Cleaned_data_path,index=False)

        except Exception as e:
            logging.info('Exception occured at Data Cleaning stage')
            raise CustomException(e,sys)
