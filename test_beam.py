import apache_beam as beam
from datetime import datetime
import json

from apache_beam.transforms.trigger import  AccumulationMode
from apache_beam.transforms.window import FixedWindows

with open('data.json') as file:
    data = json.load(file)


def time_to_unix(timestamp_str):
    return datetime.fromisoformat(timestamp_str).timestamp() if "." in timestamp_str else datetime.strptime(
        timestamp_str, "%Y-%m-%dT%H:%M:%S").timestamp()


def extract_date(trip):
    starttime = trip['starttime']
    date = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%S').date()
    return date, trip


# def extract_date(trip):
#     starttime = trip['starttime']
#     hour = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%dT%H')
#     return hour, trip


def count_trips(trip_date, trips):
    count = sum(1 for _ in trips)
    return trip_date, count


def format_date(trip_date):
    return trip_date.strftime('%Y-%m-%d')


# def format_date(trip_date):
#     return trip_date.strftime('%Y-%m-%dT%H')


if __name__ == '__main__':
    with beam.Pipeline() as pipeline:
        trips = (
                pipeline
                | beam.Create(data)
                | beam.Map(extract_date)
                | beam.WindowInto(FixedWindows(60 * 60),
                                  trigger=beam.trigger.AfterWatermark(),
                                  accumulation_mode=AccumulationMode.ACCUMULATING)
                # | beam.WindowInto(FixedWindows(60 * 60),
                #                   trigger=AfterWatermark(),
                #                   accumulation_mode=AccumulationMode.ACCUMULATING)

                | beam.Map(lambda x: (format_date(x[0]), x[1]))  # Convert date object to string
                | beam.GroupByKey()  # Provide type hint for GroupByKey
                | beam.MapTuple(count_trips)
                | beam.Map(print)
        )
