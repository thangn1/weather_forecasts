import requests
import threading
import os
import glob
import json
import pymongo
from pymongo import MongoClient
import time
import pyowm
from pyowm.utils import geo

from pyowm.utils.geo import Point
from pyowm.commons.tile import Tile
from pyowm.tiles.enums import MapLayerEnum

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from PIL import Image
import io
import numpy as np

import configparser
import sys

from datetime import datetime
from datetime import timedelta

import pytemperature as pytemp

# get the zoom level from the config file
def get_zoom_level():
    config = configparser.ConfigParser()
    config.read("config.ini")
    my_zoom_level = config["zoom"]["zoom_level"]
    return str(my_zoom_level)

# get the list of layers to get maps from the config file
def get_layers():
    config = configparser.ConfigParser()
    config.read("config.ini")
    my_layers = config["layers"]["map_layers"] # get the string "clouds_new,precipitation_new,pressure_new,wind_new,temp_new"
    my_map_layers = my_layers.strip().split(",") # get the list ["clouds_new","precipitation_new","pressure_new","wind_new","temp_new"]
    return my_map_layers

# get the list of locations from the config file
def get_locations():
    config = configparser.ConfigParser()
    config.read("config.ini")
    my_locations = config["locations"]["cities"] # get the string "London,GB;Moscow,RU;Paris,FR;New York,US;Tokyo,JP"
    my_locations = my_locations.strip().split(";") # get the list ["London,GB","Moscow,RU","Paris,FR","New York,US","Tokyo,JP"]
    my_cities = [location.strip().split(',') for location in my_locations] # get the list [["London,GB"],["Moscow,RU"],["Paris,FR"],["New York,US"],["Tokyo,JP"]]
    return my_cities

# get the api key from the config.ini file in order to validate all the services used in this program
def get_api_key():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config["openweathermap"]["api"]


# downloads 5 days/3 hour forecast
def task1(city_name, country_code, api_key, owm):

    # Connect to a Mongo Database
    db = client["WeatherDB"]        # [{database_name}]
    coll = db["5_days_per_3_hours"] # [{collection_name}]

    # get the three hours forcast
    fc = owm.three_hours_forecast(city_name + "," + country_code)
    # print out an alert if the forecast will have rain and/or snow
    will_have_rain_snow(city_name,country_code,fc)

    # get a list of weather objects from the forecast
    f = fc.get_forecast()

    for weather in f:
        # get the weather data for each day and time period in the forecast
        data = json.loads(weather.to_JSON())
        # get the timestamp from each entry and convert it into a readable format
        timestamp = datetime.utcfromtimestamp(int(data["reference_time"])).strftime('%Y-%m-%d_%H:%M:%S')
        # insert the data from 5 day weather forecast into the correct collection in the database
        insert_data(coll, city_name, country_code, timestamp, data)


# downloads 16 days/daily forecast
# response.status_code returns 401. Free account is not authorized to have the 16 days/daily forecast data
def task2(city_name, country_code, api_key, owm):

    # Connect to the database and corresponding collection
    db = client["WeatherDB"]    # [{database_name}]
    coll = db["16_days_daily"]  # [{collection_name}]

    # WARNING: daily forecasts are provided for a maximum streak of 14 days since the request time
    # WARNING: current free api key does not have access to this forecast
    try:
        fc = owm.daily_forecast(city_name + "," + country_code, limit = 16)
        # print out an alert if this forecast wil have rain and/or snow
        will_have_rain_snow(city_name,country_code,fc)

        # get a list of weather objects from the forecast
        f = fc.get_forecast()

        for weather in f:
            # get the weather data for each day and time period in the forecast
            data = json.loads(weather.to_JSON())
            # get the timestamp from each entry and convert it into a readable format
            timestamp = datetime.utcfromtimestamp(int(data["reference_time"])).strftime('%Y-%m-%d_%H:%M:%S')
            # insert the data from 5 day weather forecast into the correct collection in the database
            insert_data(coll, city_name, country_code, timestamp, data)
    except Exception as e:
        print(e)



# download weather maps
def task3(layers, z, city_name, country_code, api_key, reg):

    # get latitude and longitude values for the inputted city
    geopoint = reg.geopoints_for(city_name, country=country_code)[0]
    # get tile coordinates from the longitude and latitude values above
    x_tile, y_tile = Tile.tile_coords_for_point(geopoint, float(z))

    # download maps for each layer
    for layer in layers:
        # Params: layer, z, x, y
        # {layer} layer name
        # {z} number of zoom level
        # {x} number of x tile coordinate
        # {y} number of y tile coordinate
        url = "https://tile.openweathermap.org/map/{0}/{1}/{2}/{3}.png?appid={4}".format(layer,z,str(x_tile),str(y_tile),api_key)
        response = requests.get(url)

        # get the content of the response in bytes and convert that into an Image
        img = Image.open(io.BytesIO(response.content))
        # save the image to map_tiles folder using matplotlib.Image.imsave
        mpimg.imsave("map_tiles/"+city_name+"_"+country_code+"_"+z+"_"+layer+".png", img)


# find and open the latest map downloaded based on timestamp
def task4():
    # get the list of all files
    list_of_files = glob.glob('map_tiles/*')
    # get the latest file from this list by timestamp
    try:
        latest_file = max(list_of_files, key=os.path.getctime)
        # open and show the file using Pillow.Image
        img = Image.open(latest_file)
        img.show()
    except:
        # This exception occurs if the folder is empty and thus 'latest_file = max(list_of_files, key=os.path.getctime)' throws an exception
        print("Check back once the map files have been saved to disk")
        # make this thread sleep for a little bit and then fetch the map again once the folder has been populated
        time.sleep(3)
        task4()


# insert the inputted data into the given collection with a primary key of "{timestamp}_{country_code}_{city_name}"
def insert_data(coll, city_name, country_code, timestamp, data):
    # insert the data if it's key is not in the database already
    # if the key is in the database already, then update the data to the new data
    coll.update_one(filter = {"_id": str("{0}_{1}_{2}".format(timestamp, country_code, city_name))}, \
    update = {"$set": {"data":data} }, \
    upsert = True)


# delete all the data from the inputted collection
def clear_data(coll):
    return coll.remove()

# print alerts if the forecast given will have rain and/or snow
def will_have_rain_snow(city_name,country_code, fc):
    if fc.will_have_rain():
        print("{0},{1} will have rain".format(city_name,country_code))
    if fc.will_have_snow():
        print("{0},{1} will have snow".format(city_name,country_code))

# delete all the maps in the map_tiles folder at the start of each run in order to get the most recent maps
def delete_maps():
    folder = 'map_tiles'
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)

# round x to the nearest multiple of 3
def myround(x):
    return int(3 * round(float(x)/3))

# number_of_days = the number of points to be plotted. This number should be >= 10 but enough data has not been collected yet
def graph_forecast(my_locations, number_of_days):
    # Connect to the Mongo Database to pull data from
    db = client["WeatherDB"]        # [{database_name}]
    coll = db["5_days_per_3_hours"] # [{collection_name}]

    # Get the timestamps that the data will be pulled from
    # This will be the x-axis in the graph
    x_axis = []

    # time delta is a one-day increment
    increment = timedelta(days=1)
    # get the current time
    time_point = datetime.utcnow()
    # datetime(yyyy, mm, dd, hh, mm, ss)

    # get the hour from utcnow()
    current_hour = time_point.time().hour
    # round this value to the nearest multiple of 3
    rounded_hour = myround(current_hour)
    #  round the time to the nearest hour multiple of 3
    time_point = time_point.replace(hour=rounded_hour, minute=0, second=0)
    # reformat it in order to match the keys in the database
    timestamp = time_point.strftime('%Y-%m-%d_%H:%M:%S')

    # this will be the first point in the graph
    x_axis.append(timestamp)

    # create the x_axis
    for i in range(number_of_days-1):
        # increment the timepoint by a day
        time_point += increment
        # convert it into the proper string for the key to search
        timestamp = time_point.strftime('%Y-%m-%d_%H:%M:%S')
        # add it to the x_axis
        x_axis.append(timestamp)

    # this will be a legend which will detail which lines correlate to which city
    legend = []

    # Each city will have its own line on the plot
    for location in my_locations:
        city_name = location[0]
        country_code = location[1]

        # add the city,country to the legend
        legend.append("{0},{1}".format(city_name, country_code))

        # Get the temperature data points for each timestamp in the x-axis
        data_points_per_city = []

        for t in x_axis:

            # find the temperature (given in Kelvin) for the corresponding key for the city, country, and timestamp
            tempK = coll.find({"_id":str("{0}_{1}_{2}".format(t, country_code, city_name))})[0]["data"]["temperature"]["temp"]
            # convert this temperature into farenheit
            tempF = pytemp.k2f(tempK)

            # add this temperature to the data points
            # add this temperature to the list of datapoints
            data_points_per_city.append(tempF)

        # plot the line representing the set of temperatures for each city
        plt.plot(x_axis, data_points_per_city)

    # show the legend on the graph
    plt.legend(legend, loc='best')
    # rotate the labels of the x_axis
    plt.xticks(rotation=45)
    # show the graph
    plt.show()


def main():

    # get the api key from the config.ini file
    api_key = get_api_key()

    # get the special pyowm objects.
    owm = pyowm.OWM(api_key)  # You MUST provide a valid API key
    am = owm.alert_manager()
    reg = owm.city_id_registry()

    # names of the various map layers
    map_layers = get_layers()

    # zoom level for the maps
    zoom_level_param = get_zoom_level()

    # delete all the maps saved in the map_tiles folder before reading and saving new ones in order to reset them
    delete_maps()

    # get the list of locations from the config file
    my_locations = get_locations()

    for location in my_locations:

        city_name = location[0]
        country_code = location[1]

        # creating threads
        t1 = threading.Thread(target=task1, name="5per3hours", args=(city_name, country_code,api_key,owm,))
        t2 = threading.Thread(target=task2, name="16perday", args=(city_name, country_code,api_key,owm,))
        t3 = threading.Thread(target=task3, name="downloadweathermaps", args=(map_layers, zoom_level_param, city_name, country_code,api_key, reg,))
        t4 = threading.Thread(target=task4, name="openlatestmap")

        # starting threads
        t1.start()
        t2.start()
        t3.start()
        t4.start()

        # wait until all threads finish
        t1.join()
        t2.join()
        t3.join()
        t4.join()

        time.sleep(3)

    # graph the temperatures for the next n days
    graph_forecast(my_locations, 5)



if __name__ == "__main__":
    # establish connection to mongo database
    client = MongoClient("mongodb+srv://user_01_rw:30173ZCQXIEcHLCt@weathercluster-ren0h.mongodb.net/test?retryWrites=true")
    main()

    client.close()
