#M2 Data Science ,Data stream
#Student : GHRIBI Saif Eddine

import json
import time
import urllib.request

# import the producer
from kafka import KafkaProducer


API_KEY = "aa915894292be5171dee5c8711b39897e899aea3"
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

# Create the prodcuer
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# initiate the dicionnary that will store the bikes number of stations
Bikes_number = {}

while True:
    # get response
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())

    for station in stations:
        # retrieve the station bike number
        station_bike_number = station['available_bikes']
        # get the station number and name
        key = "{},{}".format(station["number"], station["contract_name"])
        # add stations to the Bikes_number dictionnary
        if key not in Bikes_number:
            Bikes_number[key] = station_bike_number
        # get station informations
        s_name, s_address, s_city = station["name"], station["address"], station["contract_name"]

        # if the station becomes empty
        if (station_bike_number == 0 and Bikes_number[key] > 0):
            print(" ---> The station : {} at address :  {} {} ---> becomes empty".format(
                s_name, s_city, s_address))
            producer.send("empty-stations",
                          json.dumps(station).encode())
            print()
        # if the station is no longer empty
        if (station_bike_number > 0 and Bikes_number[key] == 0):
            print(" ---> The station : {} at address :  {} {} ---> is no longer empty".format(
                s_name, s_city, s_address))
            producer.send("empty-stations",
                          json.dumps(station).encode())
            print()

        # update the number of bikes
        Bikes_number[key] = station_bike_number

    time.sleep(1)