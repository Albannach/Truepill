import csv, json
import apache_beam as beam
from apache_beam import Map, GroupByKey, CombineValues
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText

class ETLOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser): 
    parser.add_argument('--input-csv', required=True)
    parser.add_argument('--output-dir', default="")
    parser.add_argument('--json', default='price-paid')
    parser.add_argument('--averages', default='averages')
    parser.add_argument('--grouped', default='town')
    
class GroupedPrice(beam.DoFn):
    def process(self, element, key):
        j = json.loads(element)
        return [(j[key], int(j["price_paid"]))]

def toJSON(element):
    keys = ["transaction_id", "price_paid", "transaction_date", "postcode", "property_type", "new_build", "estate_type", "paon", "saon", "street", "locality", "town", "district", "county", "record_status", "transaction_category"]
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
        return json.dumps(dict(zip(keys, line)))

beam_options = ETLOptions()

print("Start  : ", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

with beam.Pipeline(options=beam_options) as pipeline:

    opt = beam_options.view_as(ETLOptions)

    rows = (pipeline
            | 'Read input csv' >> ReadFromText(opt.input_csv)
            | 'Map to JSON' >> Map(toJSON)
            )

    parsed_csv = (rows
                 | 'Write JSON' >> WriteToText(opt.output_dir+opt.json)#
                 )

    prices = (rows 
              | beam.ParDo(GroupedPrice(), opt.grouped)
              | "Grouping" >> GroupByKey()
              | "Calculating average" >> CombineValues(beam.combiners.MeanCombineFn())
              | 'Write averages' >> WriteToText(opt.output_dir+opt.averages)
              )

print("Finish : ", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
