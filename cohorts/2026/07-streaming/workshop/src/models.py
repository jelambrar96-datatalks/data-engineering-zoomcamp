import json
from dataclasses import dataclass


@dataclass
class Ride:

    PULocationID: int
    DOLocationID: int
    
    trip_distance: float
    passenger_count: int
    
    total_amount: float
    tip_amount: float
    
    lpep_pickup_datetime: int  # epoch milliseconds
    lpep_dropoff_datetime: int  


def ride_from_row(row):
    return Ride(
        
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        
        trip_distance=float(row['trip_distance']),
        passenger_count=int(row['passenger_count']),
        
        total_amount=float(row['total_amount']),
        tip_amount=float(row['tip_amount']),

        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
