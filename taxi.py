import argparse
import logging
import json

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, AfterWatermark
from apache_beam.transforms.trigger import Repeatedly, AfterCount

from geopandas import points_from_xy,read_file

from apache_beam.io.gcp.internal.clients import bigquery


def log_row(row):
    print(row)
    return row

class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam):
        x["window_start"] = window.start.to_utc_datetime()
        x["window_end"] = window.end.to_utc_datetime()
        yield x



class BoroughLocator(beam.DoFn):
    def __init__(self):
        self.borough_shapes = read_file('gs://fdaistreaming23/taxi/borough_boundaries/borough_boundaries.geojson')

    def process(self, x):
        latitude = x["latitude"]
        longitude = x["longitude"]
        point = points_from_xy([longitude], [latitude])
        for _, row in self.borough_shapes.iterrows():
            if row.geometry.contains(point[0]):
                x["borough"] = row.boro_name
                yield x
                return
        x["borough"] = "unknown"
        yield x
        return


def encode_data(data):
    key, value = data
    data_dict = {'borough': key, 'value': value}
    return data_dict



def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=pipeline_options)

    table_spec = bigquery.TableReference(
        projectId='fdaistreaming23',
        datasetId='taxi',
        tableId='taxi-rides-by-boroughs')

    schema = {'fields': [{'name': 'borough', 'type': 'STRING', 'mode': 'NULLABLE'},
                         {'name': 'value', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                         {'name': 'window_start', 'type': 'DATETIME', 'mode': 'REQUIRED'},
                         {'name': 'window_end', 'type': 'DATETIME', 'mode': 'REQUIRED'}]
    }

    rides = (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription="projects/fdaistreaming23/subscriptions/taxi",
                                                    timestamp_attribute="ts",
                                                    with_attributes=False
                                                    )
    )

    rides_windowed = (
                    rides
                    | "Parse JSON payload" >> beam.Map(json.loads)
                    | "Window into fixed windows" >> beam.WindowInto(beam.window.FixedWindows(300),
                                                             trigger=AfterWatermark(early=AfterProcessingTime(5)),
                                                             allowed_lateness=300,
                                                             accumulation_mode=AccumulationMode.DISCARDING)
                    | "BoroughLocator" >> beam.ParDo(BoroughLocator())
                    | "Key Value Pairs" >> beam.Map(lambda x: (x["borough"], x["meter_increment"]))
                    | "Sum" >> beam.CombinePerKey(sum)
                    | beam.Map(encode_data)
                    | beam.ParDo(AddWindowInfo())
                    #| "logging info" >> beam.Map(log_row)
                    | beam.io.WriteToBigQuery(table_spec,
                                      schema=schema,
                                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
