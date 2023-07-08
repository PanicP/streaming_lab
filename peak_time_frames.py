from __future__ import print_function

import argparse
import logging

import apache_beam as beam
import json

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.transforms.window import FixedWindows

def print_row(row):
    print(row)
    return row

def log_row(row):
    logging.info(str(row))
    return row

def format_output(element):
    month, average_duration = element
    result = {'month': month, 'average_duration': average_duration}
    return result


class CalculateAverageDurationFn(beam.DoFn):
    def __init__(self):
        self.total_duration = 0
        self.count = 0

    def process(self, element):
        trip_duration = element['tripduration']
        self.total_duration += trip_duration
        self.count += 1

    def finish_bundle(self):
        if self.count > 0:
            average_duration = self.total_duration / self.count
            yield beam.utils.windowed_value((self.window.start, average_duration), window=self.window)
        self.total_duration = 0
        self.count = 0


def main(argv=None):

    file_path = "data.json"
    pubsub_topic = "projects/panicp1/topics/citybike_trips"
    input_query = """
       SELECT
           tripduration,
           starttime,
           stoptime,
           start_station_latitude,
           start_station_longitude,
           end_station_latitude,
           end_station_longitude
       FROM
           `bigquery-public-data.new_york_citibike.citibike_trips`
       where TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) < CURRENT_DATE()
       and TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) > TIMESTAMP_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
       order by TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) asc
       LIMIT 100
       """

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline = beam.Pipeline(options=pipeline_options)

    # read_data = (
    #     pipeline
    #     | 'ReadFromFile' >> beam.io.ReadFromText(file_path)
    #     | 'DecodeJson' >> beam.Map(lambda line: json.loads(line))
    #     | 'logging ginfo' >> beam.Map(log_row)
    #     | 'WindowIntoFixedWindows' >> beam.WindowInto(FixedWindows(10 * 60))  # 10-minute windows
    #     | 'CalculateAverageDuration' >> beam.ParDo(CalculateAverageDurationFn())
    #     | 'FormatOutput' >> beam.Map(format_output)
    #     | 'EncodeToJson' >> beam.Map(lambda row: json.dumps(row).encode('utf-8'))
    #     | 'WriteToPubSub' >> beam.io.WriteToPubSub(topic=pubsub_topic)
    # )

    read_data = (
            pipeline
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
            | 'WindowIntoFixedWindows' >> beam.WindowInto(FixedWindows(10 * 60))  # 10-minute windows
            # | 'Console Log' >> beam.Map(print_row)
            | 'EncodeToBytes' >> beam.Map(lambda row: str(row).encode('utf-8'))
            | 'WriteToPubSub' >> beam.io.WriteToPubSub(topic=pubsub_topic)
    )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    print('Job is running')
    main()
