### Transfer MongoDB data to BigQuery

#### Operands:
	mongodb-to-bigquery.sh [OPTIONS] MONGODB_URI MONGODB_COLLECTION PROJECTID DATASET TABLE
#### Options:
	-d/--data-file <file>			Use a local JSONL file instead of retrieving data from MongoDB
	-r/--data-dir <dir>			Directory to store (temporary) data file
	* Limit data retrieval from MongoDB:
  	    -q/--query-file <file>		Use query in provided file
  	    -i/--incremental-id <id>		Only retrieve records after the given ObjectID
  	    -t/--incremental-time <timestamp>	Only retrieve records created after the given timestamp since epoch
	* Limit field retrieval from MongoDB:
  	    -f/--fields <fields>		Fields to include in the export
  	    --field-file <file>			File with fields to include in the export (1 field per line)
	* Schema definition:
  	    -b/--infer-schema-bigquery		Let BigQuery infer schema (on a sample of 100)
  	    -l/--infer-schema-local		Infer schema locally (on full dataset)
  	    -s/--schema-file <file>		Use schema in provided file
	-c/--google-cloud-storage <bucket>	Stage data in given Google Cloud Storage bucket before loading into BigQuery
	-p/--time-partitioning <field>		Set time partioning on given field
	--illegal-char-replacement <char>	Character to replace illegal characters with. Replacement must be letter, number or underscore
	* Sanitization (comply with BigQuery column name requirements):
  	    -z/--sanitize-with-regex		Use regex to sanitize column names
  	    --sanitize-with-jq			Use `jq` to sanitize column names
	-a/--allow-bad-records			Skip bad records when loading data into BigQuery
	-e/--remove-table-if-exists		Remove the table if it already exists
  
#### Examples:
	mongodb-to-bigquery.sh "mongodb://user:pass@localhost:27017/db" mycollection myproject mydataset mytable
	mongodb-to-bigquery.sh --data-file my_data.json -b myproject mydataset mytable
