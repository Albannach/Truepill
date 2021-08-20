# Truepill
Truepill Data Engineering task

The task was to transform an input file (.csv) into a newline delimited output file (.json) using Python and Apache Beam.

The data used in the task was taken from the 'https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads' site.

This data in CSV format concerns the prices paid for dwellings in England and Wales in a certain timeframe.

The output in JSON format should be the same data, with the addition of a schema (not present on the CSV data) which was derived from the PPD Linked Data definition associated with the dataset.

The columns in this flat schema are shown below...

["transaction_id", "price_paid", "transaction_date", "postcode", "property_type", "new_build", "estate_type", "paon", "saon", "street", "locality", "town", "district", "county", "record_status", "transaction_category"]

The Python code in 'beamish.py' will read a downloaded dataset from the local file system in CSV format, identifed via the '--input-csv' option (which has no default and is required) and, using Apache Beam, transform it and write it into the directory identified via the --outout-dir option (which defaults to the current directory) as a file in JSON format.  The name of the output file is of the form 'aa-00000-of-000001' where the 'aa' is a configurable prefix, the '--json' option; this defaults to 'price-paid' if not supplied.

The code also performs a grouping and averaging of the 'price-paid' column in the dataaset where the grouping can be on any other single column in the dataset, identified by the '--grouped' option.  The output containing the averages is written to a file  with name of the form 'bb-00000-of-00001', where 'bb' is a configurable prefix, the '--averages' option; this defaults to 'averages' if not supplied.

An invocation of the code is of the form shown below.

    python beamish.py --input-csv <path-to-csv>

where <path-to-csv> is the location of the CSV format file on the local file system or a location in Google Cloud Storage i.e. 'gs://.....', and the averaging is being done by 'town' by default.

GCP Dataflow
============

The application can be in a GCP Dataflow service as illustrated in the file 'dataflow.bat'.  There is quite a bit of setup required in order to do this, but it is well documented on the GCP site.

NB Before running the application, you must set an environment variable as show below...

    set GOOGLE_APPLICATION_CREDENTIALS=<path-to-credentials-file>

where <path-to-credentials-file> is the location of the credentials file created whenyoou created the project to host the Dataflow service.

    dataflow.bat
    ============

    python beamish.py^
    --runner DataflowRunner^
    --save_main_session^
    --project truepill-001^
    --region europe-west1^
    --input-csv gs://zydeco-truepill-001/data/first10.csv^
    --output-dir gs://zydeco-truepill-001/results/^
    --temp_location gs://zydeco-truepill-001/tmp/

 Here 'truepill-001' is the id of the GCP project and 'gs://zydeco-truepill-001' is a GCP bucket created to hold the input data and the results of the run.

