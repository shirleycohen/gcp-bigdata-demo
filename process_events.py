from __future__ import absolute_import

import argparse
import logging
import datetime as dt
import time as t
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT = "chicago-traffic-tracker-demo"
TOPIC = "chicago-traffic-topic"
DATASET = "chicago_traffic_data"
TABLE = "streaming_events"
TABLE_SCHEMA = ('seqnum:integer, ptime:datetime, etime:datetime, segment_id:integer, '
                'speed:integer, street:string, from_street:string, to_street:string, '
                'street_heading:string, direction:string, length:float, start_latitude:float, '
                'end_latitude:float, start_longitude:float, end_longitude:float, comments:string')

class TransformDoFn(beam.DoFn):
  def process(self, msg_bytes, window=beam.DoFn.WindowParam):

    print "***processing: " + msg_bytes.decode("utf-8")
    msg_str = msg_bytes.decode("utf-8")
    elements = msg_str.split(",")

    # get processing time (required format: 2018-02-01T09:00:00)
    ts = t.time()
    ptime = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')

    record = [{'seqnum': int(elements[0]), 'ptime' : ptime, 'etime' : elements[1], 'segment_id' : int(elements[2]),
               'speed' : int(elements[3]), 'street' : elements[4], 'from_street' : elements[5],
               'to_street' : elements[6], 'street_heading' : elements[7], 'direction' : elements[8],
               'length' : float(elements[9]), 'start_latitude' : float(elements[10]),
               'end_latitude' : float(elements[11]), 'start_longitude' : float(elements[12]),
               'end_longitude' : float(elements[13]), 'comments' : elements[14]}]

    return record


def run(argv=None):
  """Main entry point; defines and runs the pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      default='projects/' + PROJECT + '/topics/' + TOPIC,
                      help='Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>".')
  parser.add_argument('--output_table',
                      dest='output_table',
                      default=PROJECT + ':' + DATASET + '.' + TABLE,
                      help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # Local runner: DirectRunner
      # Dataflow runner: DataflowRunner
      '--runner=DirectRunner',
      '--project=chicago-traffic-tracker-demo',
      '--staging_location=gs://gs-scratch-space/tmp',
      '--temp_location=gs://gs-scratch-space/tmp',
      '--job_name=process-events-job',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:

    # transform the events into a PCollection.
    transformed = (p | 'read' >> beam.io.ReadStringsFromPubSub(known_args.input_topic)
                | beam.WindowInto(window.FixedWindows(60, 0))
                | 'transform' >> (beam.ParDo(TransformDoFn())))


    # Write the output to a text file in the output bucket
    transformed | 'write' >> beam.io.WriteToBigQuery(
       known_args.output_table,
       schema=TABLE_SCHEMA,
       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
   logging.getLogger().setLevel(logging.ERROR)
   run()


