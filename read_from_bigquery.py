import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def log_row(row):
    logging.info(str(row))
    return row

def main(argv=None):

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    input_query = """
    SELECT trip_id,			
    subscriber_type,	
    bikeid,
    TIMESTAMP_ADD(DATETIME(start_time), INTERVAL 1 YEAR) as `timestamp`,
    start_station_id,
    start_station_name,
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` 
    where TIMESTAMP_ADD(DATETIME(start_time), INTERVAL 1 YEAR) < CURRENT_DATE()
    and TIMESTAMP_ADD(DATETIME(start_time), INTERVAL 1 YEAR) > TIMESTAMP_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    order by TIMESTAMP_ADD(DATETIME(start_time), INTERVAL 1 YEAR) asc
    """

    pipeline =  beam.Pipeline(options=pipeline_options)

    read_data = (
                pipeline
                | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query,use_standard_sql=True)
                | "logging ginfo" >> beam.Map(log_row)
    )


    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
