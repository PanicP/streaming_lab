import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# def log_row(row, state):
#     start_station_name = row['start_station_name']
#     state[start_station_name] = state.get(start_station_name, 0) + 1
#     logging.info(str(row))
#     return row

# def log_row(row):
#     logging.info(str(row))
#     return row

def log_row(row, state=None):
    if 'start_station_name' in row:
        start_station_name = row['start_station_name']
        if state is not None:
            state[start_station_name] = state.get(start_station_name, 0) + 1
        logging.info(str(row))
    return row

# def find_popular_start_station(state):
#     popular_start_station = max(state, key=state.get)
#     return popular_start_station

def find_popular_start_station(state):
    print('state', state)
    if not isinstance(state, dict) or not state:
        return None  # or return a default value or handle it accordingly
    popular_start_station = max(state, key=state.get)
    return popular_start_station


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
        # | "logging ginfo" >> beam.Map(log_row, state=beam.pvalue.AsDict())
        | "logging ginfo" >> beam.Map(log_row)
    )

    result = pipeline.run()
    result.wait_until_finish()

    state = result.state
    popular_start_station = find_popular_start_station(state)
    logging.info("Popular start station: %s", popular_start_station)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
