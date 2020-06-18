# requires: Python package `bigquery-schema-generator` in `venv`; `google-cloud-sdk` in $PATH

DELETE_FILE=false
USE_LOCAL_FILE=false
LOCAL_FILE=
USE_BIGQUERY_SCHEMA_INFERENCE=false
USE_LOCAL_SCHEMA_INFERENCE=false
USE_LOCAL_SCHEMA_FILE=false
LOCAL_SCHEMA_FILE=

while :; do # https://unix.stackexchange.com/a/331530 http://mywiki.wooledge.org/BashFAQ/035
  case $1 in
  -d | --delete)
    DELETE_FILE=true
    ;;
  -f | --data-file)
    USE_LOCAL_FILE=true
    if [ "$2" ]; then
      LOCAL_FILE=$2
      shift
    else
      die 'ERROR: "--data-file" requires a non-empty option argument.'
    fi
    ;;
  -b | --infer-schema-bigquery)
    USE_BIGQUERY_SCHEMA_INFERENCE=true
    ;;
  -l | --infer-schema-local)
    USE_LOCAL_SCHEMA_INFERENCE=true
    ;;
  -s | --schema-file)
    USE_LOCAL_SCHEMA_FILE=true
    if [ "$2" ]; then
      LOCAL_SCHEMA_FILE=$2
      shift
    else
      die 'ERROR: "--schema-file" requires a non-empty option argument.'
    fi
    ;;
  -q | --query-file)
    USE_QUERY_FILE=true
    if [ "$2" ]; then
      QUERY_FILE=$2
      shift
    else
      die 'ERROR: "--query-file" requires a non-empty option argument.'
    fi
    ;;
    -i | --incremental-id)
    USE_START_ID=true
    if [ "$2" ]; then
      START_ID=$2
      shift
    else
      die 'ERROR: "--incremental-id" requires a non-empty option argument.'
    fi
    ;;
  -t | --incremental-time)
    USE_START_TIME=true
    if [ "$2" ]; then
      START_TIME=$2
      shift
    else
      die 'ERROR: "--incremental-time" requires a non-empty option argument.'
    fi
    ;;
 *) break ;;
  esac
  shift
done

MONGO_URI=$1
MONGO_COLLECTION=$2
BQ_PROJECTID=$3
BQ_DATASET=$4
BQ_TABLE=$5

# TODO print progress

if [ "${USE_LOCAL_FILE}" = true ]; then
  DATA_FILENAME="${LOCAL_FILE}"
else
  DATA_FILENAME="${MONGO_COLLECTION}".json.gz
fi
if [ "${USE_LOCAL_SCHEMA_FILE}" = true ]; then
  SCHEMA_FILENAME="${LOCAL_SCHEMA_FILE}"
else
  SCHEMA_FILENAME="${MONGO_COLLECTION}".schema.json
fi

if [ "${USE_LOCAL_FILE}" = false ]; then
  MONGO_COMMAND="mongoexport --uri=${MONGO_URI} --collection=${MONGO_COLLECTION} "
  if [ "${USE_QUERY_FILE}" = true ]; then
    MONGO_COMMAND+="--query=$(cat "${QUERY_FILE}")"
  elif [ "${USE_START_ID}" = true ]; then
    MONGO_COMMAND+="--query='{ \"_id\": { \"\$gte\": ${START_ID} } }'"
  elif [ "${USE_START_TIME}" = true ]; then
    MONGO_COMMAND+="--query='{ \"_id\": { \"\$gte\": $(printf '%x\n' "${START_TIME}")0000000000000000 } }'"
    # https://stackoverflow.com/a/8753670/7391782
  fi
  MONGO_COMMAND+=" | sed 's/\"\$date\"/\"date\"/g;s/\"\$oid\"/\"oid\"/g'" # BigQuery doesn't accept fields with dollar signs
  MONGO_COMMAND+=" | gzip" # gzip to reduce data size
  # TODO gzip to disk for space, but upload uncompressed for faster processing?
  # https://cloud.google.com/bigquery/docs/loading-data#loading_compressed_and_uncompressed_data
  # https://www.oreilly.com/library/view/google-bigquery-the/9781492044451/ch04.html
  MONGO_COMMAND+="> ${DATA_FILENAME}"
  eval "$MONGO_COMMAND"
else
  if [ ! -f "${DATA_FILENAME}" ]; then
    die 'ERROR: data file does not exist'
  fi
fi

# TODO tail json file to retrieve last ID? not necessarily sorted!

if [ $USE_LOCAL_SCHEMA_INFERENCE = true ]; then
  # infer BigQuery schema across whole file (not 100 record sample) using https://pypi.org/project/bigquery-schema-generator/
  zcat "${DATA_FILENAME}" | venv/bin/generate-schema >"${SCHEMA_FILENAME}"
  # TODO `head` to make local inference faster?
fi
if [ ! -f "${SCHEMA_FILENAME}" ]; then
  die 'ERROR: schema file does not exist'
fi


# TODO make dataset if necessary?
# bq --location=EU mk --dataset ${BQ_PROJECTID}:${BQ_DATASET}

# TODO stage in Google Cloud Storage? Maximum file size for compressed JSON is 4 GB (https://cloud.google.com/bigquery/quotas#load_jobs)

BQ_COMMAND="bq load --source_format NEWLINE_DELIMITED_JSON --ignore_unknown_values "
if [ $USE_BIGQUERY_SCHEMA_INFERENCE = true ]; then
  BQ_COMMAND+="--autodetect "
else
  BQ_COMMAND+="--schema ${SCHEMA_FILENAME} "
fi
BQ_COMMAND+="--project_id=${BQ_PROJECTID} ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ${DATA_FILENAME}"
eval "$BQ_COMMAND"

# TODO # set up partitioned table

if [ $DELETE_FILE = true ]; then
  rm "${DATA_FILENAME}"
  if [ $USE_BIGQUERY_SCHEMA_INFERENCE = false ]; then
    rm "${SCHEMA_FILENAME}"
  fi
fi

# TODO save timestamp/ObjectID of last Mongo record? for incremental upload

#  https://medium.com/google-cloud/export-load-job-with-mongodb-bigquery-part-i-64a00eb5266b
#  https://hevodata.com/blog/mongodb-to-bigquery-etl-stream-data/
#  https://stackoverflow.com/questions/42167543/mongodb-to-bigquery
