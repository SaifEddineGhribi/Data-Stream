#M2 Data Science ,Data stream
#Student : GHRIBI Saif Eddine

import json
from kafka import KafkaConsumer

#create a consumer 
consumer = KafkaConsumer("empty-stations", bootstrap_servers='localhost:9092',
                         group_id="velib-monitor-stations")

#process messages from consumer                         
for message in consumer:
    station = json.loads(message.value.decode())

    #get station information    
    current_number_bikes,s_city,s_address  =int(station["available_bikes"]) , station["contract_name"],station["address"]
    
    #if the station becomes empty 
    if current_number_bikes == 0:
        print("The station at [ Address : {} ] and  [City : {}] becomes empty ".format(s_address,s_city))
        print('')
