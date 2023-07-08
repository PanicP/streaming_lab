import argparse
import logging
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode, AfterProcessingTime
from apache_beam.transforms.window import FixedWindows


import apache_beam as beam
import json

from apache_beam.options.pipeline_options import PipelineOptions

# def log_row(row):
#     logging.info(str(row))
#     return row

def filter_peak_hours(row):
    from datetime import datetime
    start_time = datetime.fromisoformat(row['starttime'])
    if start_time.hour >= 7 and start_time.hour < 9 or start_time.hour >= 17 and start_time.hour < 19:
        return row

def count_stations(row):
    return row['start_station_name'], 1

def extract_station_count(result_tuple):
    station, count = result_tuple
    return station, count

def find_popular_start_stations(elements):
    sorted_stations = sorted(elements, key=lambda x: x[1], reverse=True)
    return sorted_stations[:10]

def publish_result(result, pubsub_topic):
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(pubsub_topic)

    message = ', '.join([station for station, count in result])
    publisher.publish(topic_path, message.encode())

def assign_time_stamp(row):
    import apache_beam as beam
    return beam.window.TimestampedValue(row, row['starttime'])


def main(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    input_query = """
    SELECT
        *
    FROM
        `bigquery-public-data.new_york_citibike.citibike_trips`
    WHERE
        tripduration IS NOT NULL
        AND
        TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) < CURRENT_DATE()
        AND
        TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) > TIMESTAMP_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ORDER BY TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) ASC
    """

    pubsub_topic = "projects/panicp1/topics/citybike_trips"

    window_duration = 2 * 60 * 60  # 2 hours in seconds
    pipeline = beam.Pipeline(options=pipeline_options)

    read_data = (
            pipeline
            # | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
            | 'ReadFromJSON' >> beam.io.ReadFromText('./data.json')
            # | "FilterPeakHours" >> beam.Filter(filter_peak_hours)
            | "AssignTimestamps" >> beam.Map(assign_time_stamp)
            | "FixedWindows" >> beam.WindowInto(FixedWindows(window_duration),
                                                trigger=AfterWatermark(early=AfterProcessingTime(10)),
                                                accumulation_mode=AccumulationMode.DISCARDING)
            | "CountStations" >> beam.Map(count_stations)
            | 'CombinePerKey' >> beam.CombinePerKey(beam.combiners.CountCombineFn())
            | 'ExtractStationCount' >> beam.Map(extract_station_count)
            | 'FindPopularStartStations' >> beam.Map(find_popular_start_stations)
            | 'WriteToPubSub' >> beam.io.WriteToPubSub(topic=pubsub_topic)
    )
    # read_data = (
    #     pipeline
    #     | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
    #     | "FilterPeakHours" >> beam.Filter(filter_peak_hours)
    #     # | "LoggingInfo" >> beam.Map(log_row)
    #     | "CountStations" >> beam.Map(count_stations)
    #     | 'CombinePerKey' >> beam.CombinePerKey(beam.combiners.CountCombineFn())
    #     | 'ExtractStationCount' >> beam.Map(extract_station_count)
    #     | 'FindPopularStartStations' >> beam.Map(find_popular_start_stations)
    #     | 'WriteToPubSub' >> beam.io.WriteToPubSub(topic=pubsub_topic)
    # )


    result = pipeline.run()
    result.wait_until_finish()

    # popular_start_stations = result.state[None]
    # publish_result(popular_start_stations, pubsub_topic)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()

