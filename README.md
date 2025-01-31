### This is a take home task for hello fresh technical round, the project reads files consisting of delivery data and then updates the ice pack required for the orders using external APIs

### How to run the files:
1. Pull the repo
2. Make a virtual environment python -m venv env
3. Activate the virtual environment env\Scripts\activate
4. pip install -r requirements.txt
5. Run the application using the following command python main.py
6. Optionally run the docker file docker build -t processor:v1
7. Docker run docker run processor:v1

The main.py runs the scripts in the DataProcessor.py file importing the class DataProcessor from the DataProcessor.py file this is done by executing a method called process_dataframe() from the DataProcessor class.
process_dataframe has the flow of execution.

Here is a sequence:
1. The files are loaded when the class is initialized along with the API keys.
2. process_dataframe method is called
3. The column 'EXPECTED_DELIVERY_DAYS' is transformed into the proper date time format for future use
4. A new columnn "OUTCODE", The "OUTCODE" column consists of everything other than the last 3 letters/digits of the post code, the method get_outcode also strips any spaces
5. New Columns Latitude and Longitude are derived from get_coordinates('OUTCODE') using the openstreetmap API.
6. insert_latlong method updates the latitutde and longitude for the outcodes which aren't available from the openstreet API.
7. fill_dataframe_with_station_code_and_temperature updates the station_code using the latitude and longitude from meteostat api then the average_temperature is updated using the station_code via meteostat API.
9. fill_dataframe_temperature_nan uses the temperature from the nearest available location on the given day to update the areas for which the station code gave no response to the api request.
10. Once this is done allocate_ice_packs_to_orders method updates the Ice pack required column using the temperature interval table and transit days.
