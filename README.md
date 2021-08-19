# Truepill
Truepill Data Engineering task

The task was to an input file (.csv) into a newline delimited output file (.json) using Python and Apache Beam.

The data used in the task was taken from the 'https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads' site.

This data in CSV format concerns the prices paid for dwellings in England and Wales in a certain timeframe.

The output in JSON format should be the same data, with the addition of a schema (not present on the CSV data) which was derived from the PPD Linked Data definition associated with the dataset.

The columns in this flat schema are shown below...

["transaction_id", "price_paid", "transaction_date", "postcode", "property_type", "new_build", "estate_type", "paon", "saon", "street", "locality", "town", "district", "county", "record_status", "transaction_category"]

The Python code in 'beamish.py' will read a downloaded dataset from the local file system in CSV format, identifed via the '--csv-input' option (which has no default and is required) and, using Apache Beam, transform it and write it into the current directory as a file in JSON format.  The name of the output file is of the form 'aa-00000-of-000001' where the 'aa' is a configurable prefix, the '--output-prefix' option; this defaults to 'xx' if not supplied.

The code also performs a grouping and averaging of the 'price-paid' column in the dataaset where the grouping can be on any other single column in the dataset, identified by the '--grouped' option.  The output containing the averages is to a file  with name of the form 'bb-00000-of-00001', where 'bb' is a configurable prefix, the '--average-prifix' option; this defaults to 'yy' if not supplied.

An invocation of the code is of the form shown below.

    python beamish.py --csv-input <path-to-csv> --grouped county --output-prefix price-paid --average-prefix by-county

where <path-to-csv> is the location of the CSV format file on the local file system, and the averaging is being done by county.

The <path-to-csv> could also be a location in Google Cloud Storage i.e. 'gs://.....', but that hasn't been tested.

