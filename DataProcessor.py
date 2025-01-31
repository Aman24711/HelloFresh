import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

class DataProcessor:
    def __init__(self, csv_path="Temperature_bands.csv", parquet_path="data_eng_test.parquet"):
        self.RAPIDAPI_KEY1 = os.getenv('RAPIDAPI_KEY1')
        self.RAPIDAPI_KEY2 = os.getenv('RAPIDAPI_KEY2')
        self.df_csv = pd.read_csv(csv_path)
        self.df_parquet = pd.read_parquet(parquet_path, engine="pyarrow")


    def process_dataframe(self, output_file_path = 'ice_pack_requirement_for_orders.parquet'):
        self.df_parquet['EXPECTED_DELIVERY_DATE'] = pd.to_datetime(self.df_parquet['EXPECTED_DELIVERY_DATE']).dt.strftime('%Y-%m-%d')
        self.df_parquet["OUTCODE"] = self.df_parquet["POSTCODE"].str[:-3]
        self.df_parquet[['LATITUDE', 'LONGITUDE']] = self.df_parquet['OUTCODE'].apply(lambda x: pd.Series(self.get_coordinates(x)))
        self.df_parquet = self.insert_latlong(self.df_parquet)
        self.df_parquet = self.fill_dataframe_with_station_code_and_temperature(self.df_parquet)
        self.df_parquet = self.fill_dataframe_temperature_nan(self.df_parquet)
        self.df_parquet = self.allocate_ice_packs_to_orders(self.df_parquet)
        self.df_parquet.to_parquet(output_file_path)

    def get_outcode(self, postcode):
        if len(postcode) < 5:
            raise ValueError
        postcode = postcode.replace(' ', '')
        return postcode[:-3]
    
    def get_coordinates(self,postcode):
        url = f"https://nominatim.openstreetmap.org/search?format=json&q={postcode}, UK"
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})

        if response.status_code == 200 and response.json():
            location = response.json()[0]
            return float(location['lat']), float(location['lon'])
        else:
            return None, None
        

    def insert_latlong(self, dataframe):
        LatLong = {'GY5':(49.4633, -2.5966),
        'GY4': (49.4336, -2.5594),
        'BT71': (54.5341, -6.6960),
        'GY8' : (49.4297, -2.6195),
        'CF38' : (51.5629, -3.3166),
        'GY2' : (49.4846, -2.5525),
        'BT82' : (54.8477, -7.3737),
        'BT93' : (54.4832, -7.9014),
        'GY1' : (49.4643, -2.5506),
        'BT63' : (54.3878, -6.3529),
        'BT82' : (54.8477, -7.3737),
        'JE3' : (49.2134,-2.1584),
        'JE2' : (49.1892, -2.0943)}

        dataframe_with_latlon = dataframe.copy()
        dataframe_with_latlon['OUTCODE'] = dataframe_with_latlon['OUTCODE'].str.upper()

        # Loop through DataFrame and update missing LATITUDE and LONGITUDE
        for index, row in dataframe_with_latlon.iterrows():
            if pd.isna(row['LATITUDE']) or pd.isna(row['LONGITUDE']):  # Only update if missing
                outcode = row['OUTCODE']
                if outcode in LatLong:  # Check if OUTCODE exists in dictionary
                    lat, lon = LatLong[outcode]
                    dataframe_with_latlon.at[index, 'LATITUDE'] = lat
                    dataframe_with_latlon.at[index, 'LONGITUDE'] = lon

        return dataframe_with_latlon
    

    def get_nearest_station(self, lat, lon):
        url = f"https://meteostat.p.rapidapi.com/stations/nearby?lat={lat}&lon={lon}&limit=1"
        headers = {
            "x-rapidapi-host": "meteostat.p.rapidapi.com",
            "x-rapidapi-key": self.RAPIDAPI_KEY1
        }
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200 and response.json().get("data"):
            return response.json()["data"][0]["id"]  # Returns the nearest station ID
        return None

    def get_temperature(self, station_id, date):
        #station_id = get_nearest_station(lat, lon)
        
        url = f"https://meteostat.p.rapidapi.com/stations/daily?station={station_id}&start={date}&end={date}"
        headers = {
            "x-rapidapi-host": "meteostat.p.rapidapi.com",
            "x-rapidapi-key": self.RAPIDAPI_KEY2
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200 and response.json().get("data"):
            return response.json()["data"][0]["tavg"]  # Returns avg temperature
        return None

    def fill_dataframe_with_station_code_and_temperature(self, dataframe):
        dataframe['STATION_ID'] = None
        dataframe['AVERAGE_TEMPERATURE'] = None

        for index, row in dataframe.iterrows():
            if dataframe.at[index, 'STATION_ID'] == None:
                lat = dataframe.at[index, 'LATITUDE']
                lon = dataframe.at[index, 'LONGITUDE']
                dataframe.at[index, 'STATION_ID'] = self.get_nearest_station(lat, lon)
            else:
                pass

        for index, row in dataframe.iterrows():
            if dataframe.at[index, 'AVERAGE_TEMPERATURE'] == None:
                date = dataframe.at[index, 'EXPECTED_DELIVERY_DATE']
                station_id = dataframe.at[index, 'STATION_ID']
                dataframe.at[index, 'AVERAGE_TEMPERATURE'] = self.get_temperature(station_id, date)
            else:
                pass

        return dataframe
    
    def fill_dataframe_temperature_nan(self, dataframe):
        filled_dataframe = dataframe.copy()

        # Convert to NumPy arrays for efficient computation
        coords = filled_dataframe[['LATITUDE', 'LONGITUDE']].to_numpy()

        # Iterate over rows where AVERAGE_TEMPERATURE is missing
        for idx, row in filled_dataframe[filled_dataframe['AVERAGE_TEMPERATURE'].isna()].iterrows():
            lat, lon, date = row['LATITUDE'], row['LONGITUDE'], row['EXPECTED_DELIVERY_DATE']

            # Compute Euclidean distance to all other points (ignoring missing temperature points)
            distances = np.sqrt((coords[:, 0] - lat) ** 2 + (coords[:, 1] - lon) ** 2)

            # Get the index of the closest point that has a temperature recorded for the same date
            df_same_date = filled_dataframe[(filled_dataframe['EXPECTED_DELIVERY_DATE'] == date) & filled_dataframe['AVERAGE_TEMPERATURE'].notna()]
            
            if not df_same_date.empty:
                df_same_date['distance'] = np.sqrt(
                    (df_same_date['LATITUDE'] - lat) ** 2 + (df_same_date['LONGITUDE'] - lon) ** 2
                )

                # Find the nearest available temperature
                nearest_idx = df_same_date['distance'].idxmin()
                filled_dataframe.at[idx, 'AVERAGE_TEMPERATURE'] = df_same_date.at[nearest_idx, 'AVERAGE_TEMPERATURE']

        return filled_dataframe
    

    def get_ice_packs(self, row):
        # Find the matching row based on temperature range
        match = self.df_csv[
            (self.df_csv['temperature_min'] <= row['AVERAGE_TEMPERATURE']) & 
            (self.df_csv['temperature_max'] > row['AVERAGE_TEMPERATURE'])
        ]
        
        # If a match is found, return the corresponding pouch size value
        if not match.empty:
            return match[row['COOL_POUCH_SIZE']].values[0]
        return None  # If no match, return None

    def get_ice_packs_XL(self, row):
        # Find the matching row based on temperature range
        match = self.df_csv[
            (self.df_csv['temperature_min'] <= row['AVERAGE_TEMPERATURE']) & 
            (self.df_csv['temperature_max'] > row['AVERAGE_TEMPERATURE'])
        ]
        
        # If a match is found, return the corresponding pouch size value
        if not match.empty:
            return match['L'].values[0] + 1
        return None  # If no match, return None
    

    def allocate_ice_packs_to_orders(self, dataframe):
        datapack_with_ice_packs = dataframe.copy()

        for index, row in datapack_with_ice_packs.iterrows():
            row['EXPECTED_DELIVERY_DATE'] = pd.to_datetime(row['EXPECTED_DELIVERY_DATE'])
            row['PRODUCTION_DATE'] = pd.to_datetime(row['PRODUCTION_DATE'])
            delivery_time = row['EXPECTED_DELIVERY_DATE'] - row['PRODUCTION_DATE']
            days = delivery_time.days
            
            if days > 1:
                transit_packs = days
            else:
                transit_packs = 0
            #print(row['EXPECTED_DELIVERY_DATE'], row['PRODUCTION_DATE']," ", row['EXPECTED_DELIVERY_DATE'] - row['PRODUCTION_DATE'], days)
            
            if datapack_with_ice_packs.at[index, 'COOL_POUCH_SIZE'] in ('S','M','L'):
                datapack_with_ice_packs.at[index, 'NUMBER_OF_ICE_PACKS'] = self.get_ice_packs(row) + transit_packs
            elif datapack_with_ice_packs.at[index, 'COOL_POUCH_SIZE'] == 'XL':
                datapack_with_ice_packs.at[index, 'NUMBER_OF_ICE_PACKS'] = self.get_ice_packs_XL(row) + transit_packs

        return datapack_with_ice_packs 
    