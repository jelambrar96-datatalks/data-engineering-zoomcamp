import json
import dataclasses

from dataclasses import dataclass

@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float

    def ride_from_row(row):
        return Ride(
            PULocationID=int(row['PULocationID']),
            DOLocationID=int(row['DOLocationID']),
            trip_distance=float(row['trip_distance']),
            total_amount=float(row['total_amount']),
        )

    def ride_serializer(self):
        ride_dict = dataclasses.asdict(self)
        ride_json = json.dumps(ride_dict).encode('utf-8')
        return ride_json

    @classmethod
    def ride_deserializer(cls, data):
        json_str = data.decode('utf-8')
        ride_dict = json.loads(json_str)
        return cls(**ride_dict)


@dataclass
class YellowRide(Ride):
    tpep_pickup_datetime: int  # epoch milliseconds

    def ride_from_row(row):
        return YellowRide(
            PULocationID=int(row['PULocationID']),
            DOLocationID=int(row['DOLocationID']),
            trip_distance=float(row['trip_distance']),
            total_amount=float(row['total_amount']),
            tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
        )



@dataclass
class GreenRide(Ride):
    lpep_pickup_datetime: int  # epoch milliseconds
    lpep_dropoff_datetime: int  # epoch milliseconds
    passenger_count: int
    tip_amount: float

    def ride_from_row(row):
        return GreenRide(
            PULocationID=int(row['PULocationID']),
            DOLocationID=int(row['DOLocationID']),
            trip_distance=float(row['trip_distance']),
            total_amount=float(row['total_amount']),
            lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
            lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
            passenger_count=int(row['passenger_count']),
            tip_amount=float(row['tip_amount']),
        )
