python beamish.py^
 --runner DataflowRunner^
 --save_main_session^
 --project truepill-001^
 --region europe-west1^
 --input-csv gs://zydeco-truepill-001/data/first10.csv^
 --output-dir gs://zydeco-truepill-001/results/^
 --temp_location gs://zydeco-truepill-001/tmp/