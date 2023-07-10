import argparse
import logging
from datetime import datetime
# from datasketch import HyperLogLog
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode, AfterProcessingTime
from apache_beam.transforms.window import FixedWindows

import apache_beam as beam
import json

from apache_beam.options.pipeline_options import PipelineOptions


def main(argv=None):
    def time_to_unix(timestamp_str):
        from datetime import datetime
        return datetime.fromisoformat(timestamp_str).timestamp() if "." in timestamp_str else datetime.strptime(
            timestamp_str, "%Y-%m-%dT%H:%M:%S").timestamp()

    def extract_date(trip):
        from datetime import datetime
        starttime = trip['starttime']
        date = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%S').date()
        return date, trip

    def count_trips(trip_date, trips):
        from datasketch import HyperLogLog
        hll = HyperLogLog(p=16)
        actual_count = sum(1 for _ in trips)
        # hll = HyperLogLog()


        for trip in trips:
            hll.update((trip['starttime']).encode('utf-8'))
        estimated_count = hll.count()
        accuracy = (estimated_count / actual_count) * 100
        return trip_date, accuracy

    def format_date(trip_date):
        return trip_date.strftime('%Y-%m-%d')

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

    pipeline = beam.Pipeline(options=pipeline_options)

    read_data = (
            pipeline
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
            | "MapDate" >> beam.Map(extract_date)
            | "FixedWindows" >> beam.WindowInto(FixedWindows(60 * 60 * 24),
                                                trigger=beam.trigger.AfterWatermark(),
                                                accumulation_mode=AccumulationMode.ACCUMULATING)
            | "MapKeyForGrouping" >> beam.Map(lambda data: (format_date(data[0]), data[1]))
            | 'GroupByKey' >> beam.GroupByKey()
            | 'MapAndCount' >> beam.MapTuple(count_trips)
            | 'EncodeToJson' >> beam.Map(lambda row: json.dumps(row).encode('utf-8'))
            | 'WriteToPubSub' >> beam.io.WriteToPubSub(topic=pubsub_topic)
    )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
