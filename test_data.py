import argparse
import logging

import apache_beam as beam
import json

from apache_beam.options.pipeline_options import PipelineOptions

def format_output(element):
    month, average_duration = element
    result = {'month': month, 'average_duration': average_duration}
    return result

def main(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

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
    where 
    tripduration is not null 
    and
    TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) < CURRENT_DATE()
    and 
    TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) > TIMESTAMP_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    order by TIMESTAMP_ADD(DATETIME(starttime), INTERVAL 6 YEAR) asc
    """

    pubsub_topic = "projects/panicp1/topics/citybike_trips"

    pipeline = beam.Pipeline(options=pipeline_options)

    read_data = (
            pipeline
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
            | 'EncodeToJson' >> beam.Map(lambda row: json.dumps(row).encode('utf-8'))
            | 'WriteToPubSub' >> beam.io.WriteToPubSub(topic=pubsub_topic)
    )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
