import csv, json
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions

class ETLOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser): 
    parser.add_argument('--csv-input', required=True)
    parser.add_argument('--output-prefix', default='xx')
    parser.add_argument('--grouped', default='town')
    parser.add_argument('--average-prefix', default='yy')

class GroupedPrice(beam.DoFn):
    def process(self, element, key):
        j = json.loads(element)
        return [(j[key], int(j["price_paid"]))]

def parse_file(element):
    keys = ["transaction_id", "price_paid", "transaction_date", "postcode", "property_type", "new_build", "estate_type", "paon", "saon", "street", "locality", "town", "district", "county", "record_status", "transaction_category"]
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        return json.dumps(dict(zip(keys, line)))

beam_options = ETLOptions()

print("Start : ", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
with beam.Pipeline(options=beam_options) as pipeline:
    rows = (pipeline
            | 'Read input csv' >> beam.io.ReadFromText(beam_options.view_as(ETLOptions).csv_input)
            | 'Map to JSON' >> beam.Map(parse_file)
            )
    parsed_csv = (rows
                 | 'Write JSON' >> beam.io.WriteToText(beam_options.view_as(ETLOptions).output_prefix)
                 )
    prices = (rows 
              | beam.ParDo(GroupedPrice(), beam_options.view_as(ETLOptions).grouped)
              | "Grouping" >> beam.GroupByKey()
              | "Calculating average" >> beam.CombineValues(beam.combiners.MeanCombineFn())
              | 'Write averages' >> beam.io.WriteToText(beam_options.view_as(ETLOptions).average_prefix)
              )

print("Finish : ", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
