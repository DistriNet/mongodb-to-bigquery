# requires: Python package `bigquery-schema-generator` in `venv`; `google-cloud-sdk` in $PATH

MONGOURI=$1
COLLECTION=$2
BQPROJECTID=$3
BQDATASET=$4
BQTABLE=$5

# TODO process arguments with getopts/getopt https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash

# TODO make file exists check optional
#if [ ! -f "${COLLECTION}".json.gz ]; then
mongoexport --uri="${MONGOURI}" --collection="${COLLECTION}" |
sed 's/"\$date"/"date"/g;s/"\$oid"/"oid"/g' | # BigQuery doesn't accept fields with dollar signs
gzip > "${COLLECTION}".json.gz # gzip to reduce data transport
#fi

# TODO support query in mongoexport   --query='{ "_id": { "$gte": 3 }, "date": { "$lt": { "$date": "2016-01-01T00:00:00.000Z" } } }'

# infer BigQuery schema across whole file (not 100 record sample) using https://pypi.org/project/bigquery-schema-generator/
zcat "${COLLECTION}".json.gz | venv/bin/generate-schema > "${COLLECTION}".schema.json
# TODO option to use BigQuery's own inference (faster?)      --autodetect
# TODO `head` to make local inference faster?

# TODO make dataset if necessary?
# bq --location=EU mk --dataset ${BQPROJECTID}:${BQDATASET}

# pipe directly to BigQuery
bq load --source_format NEWLINE_DELIMITED_JSON \
    --ignore_unknown_values \
    --schema "${COLLECTION}".schema.json \
    --project_id="${BQPROJECTID}" \
    "${BQPROJECTID}":"${BQDATASET}"."${BQTABLE}" \
    "${COLLECTION}".json.gz
# TODO upload to Google Cloud Storage? gsutil

# TODO # set up clustered table

# TODO make delete optional
rm "${COLLECTION}".json.gz "${COLLECTION}".schema.json

#  https://medium.com/google-cloud/export-load-job-with-mongodb-bigquery-part-i-64a00eb5266b
#  https://hevodata.com/blog/mongodb-to-bigquery-etl-stream-data/
#  https://stackoverflow.com/questions/42167543/mongodb-to-bigquery
