# requires: Python package `bigquery-schema-generator` in `venv`; `google-cloud-sdk` in $PATH

RED='\033[0;31m'
GREEN='\033[0;32m'
BROWN='\033[0;33m'
NC='\033[0m' # No Color

DELETE_FILE=false
USE_LOCAL_FILE=false
LOCAL_FILE=
USE_GOOGLE_CLOUD_STORAGE=false
GOOGLE_CLOUD_STORAGE_BUCKET=
USE_BIGQUERY_SCHEMA_INFERENCE=false
USE_LOCAL_SCHEMA_INFERENCE=false
USE_LOCAL_SCHEMA_FILE=false
LOCAL_SCHEMA_FILE=
USE_TIME_PARTITIONING=false
TEST_MODE=false

help () {
  printf "* Transfer MongoDB data to BigQuery *\n"
  printf "Operands:\n"
  printf "\tmongodb-to-bigquery.sh [OPTIONS] MONGODB_URI MONGODB_COLLECTION PROJECTID DATASET TABLE\n"
  printf "Options:\n"

  printf "\t-f/--data-file <file>\t\t\tUse a local JSON file instead of retrieving data from MongoDB\n"
  printf "\t* Limit data retrieval from MongoDB:
  \t    -q/--query-file <file>\t\tUse query in provided file
  \t    -i/--incremental-id <id>\t\tOnly retrieve records after the given ObjectID
  \t    -t/--incremental-time <timestamp>\tOnly retrieve records created after the given timestamp since epoch\n"
  printf "\t* Schema definition:
  \t    -b/--infer-schema-bigquery\t\tLet BigQuery infer schema (on a sample of 100)
  \t    -l/--infer-schema-local\t\tInfer schema locally (on full dataset)
  \t    -s/--schema-file <file>\t\tUse schema in provided file\n"
  printf "\t-c/--google-cloud-storage <bucket>\tStage data in given Google Cloud Storage bucket before loading into BigQuery\n"
  printf "\t-p/--time-partitioning <field>\t\tSet time partioning on given field\n"
}
die() { echo "$*" 1>&2 ; exit 1; }

###################################### Option parsing ######################################
while :; do # https://unix.stackexchange.com/a/331530 http://mywiki.wooledge.org/BashFAQ/035
  case $1 in
  -h | --help)
    help
    exit 0;
    ;;
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
  -c | --google-cloud-storage)
    USE_GOOGLE_CLOUD_STORAGE=true
    if [ "$2" ]; then
      GOOGLE_CLOUD_STORAGE_BUCKET=$2
      shift
    else
      die 'ERROR: "--google-cloud-storage" requires a non-empty option argument.'
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
  -p | --time-partitioning)
    USE_TIME_PARTITIONING=true
    if [ "$2" ]; then
      TIME_PARTITIONING_FIELD=$2
      shift
    else
      die 'ERROR: "--time-partitioning" requires a non-empty option argument.'
    fi
    ;;
  --test)
    TEST_MODE=true
    ;;
 *) break ;;
  esac
  shift
done
############################################################################################

MONGO_URI=$1
MONGO_COLLECTION=$2
BQ_PROJECTID=$3
BQ_DATASET=$4
BQ_TABLE=$5

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
  echo -e "${BROWN}[*] Retrieving data from MongoDB - collection=${MONGO_COLLECTION} @ ${MONGO_URI} ${NC}"
  MONGO_COMMAND="mongoexport --uri=${MONGO_URI} --collection=${MONGO_COLLECTION} --type json "
  if [ "${USE_QUERY_FILE}" = true ]; then
    MONGO_COMMAND+="--query='$(cat "${QUERY_FILE}")'"
  elif [ "${USE_START_ID}" = true ]; then
    MONGO_COMMAND+="--query='{ \"_id\": { \"\$gte\": ObjectId(\"${START_ID}\") } }'"
  elif [ "${USE_START_TIME}" = true ]; then
    MONGO_COMMAND+="--query='{ \"_id\": { \"\$gte\": ObjectId(\"$(printf '%x\n' "${START_TIME}")0000000000000000\") } }'"
    # https://stackoverflow.com/a/8753670/7391782
  fi
  if [ "${TEST_MODE}" = true ]; then
    MONGO_COMMAND+="--limit 10000"
  fi
  echo $MONGO_COMMAND
  # BigQuery doesn't accept fields with dollar signs
  MONGO_COMMAND+=" | sed 's/{\"\$date\":\"\([^}]*\)\"}/\"\1\"/g;s/{\"\$oid\":\"\([^}]*\)\"}/\"\1\"/g'"
  # gzip to reduce data size
  MONGO_COMMAND+=" | gzip"
  # TODO gzip to disk for space, but upload uncompressed for faster processing?
  # https://cloud.google.com/bigquery/docs/loading-data#loading_compressed_and_uncompressed_data
  # https://www.oreilly.com/library/view/google-bigquery-the/9781492044451/ch04.html
  MONGO_COMMAND+="> ${DATA_FILENAME}"
  eval "$MONGO_COMMAND"
else
  if [ ! -f "${DATA_FILENAME}" ]; then
    die 'ERROR: data file does not exist'
  fi
  echo -e "${BROWN}[*] Reading data from local file ${DATA_FILENAME} ${NC}"
fi
echo -e "${GREEN}[+] Data retrieved successfully! ${NC}"

# TODO tail json file to retrieve last ID? not necessarily sorted!
# TODO split file into smaller chunks? query in smaller chunks?

if [ $USE_LOCAL_SCHEMA_INFERENCE = true ]; then
  # infer BigQuery schema across whole file (not 100 record sample) using https://pypi.org/project/bigquery-schema-generator/
  echo -e "${BROWN}[*] Generating BigQuery schema ${NC}"
  zcat "${DATA_FILENAME}" | venv/bin/generate-schema >"${SCHEMA_FILENAME}"
  # TODO `head` to make local inference faster?
fi
if [ ! -f "${SCHEMA_FILENAME}" ]; then
  die 'ERROR: schema file does not exist'
fi
echo -e "${GREEN}[+] BigQuery schema available! ${NC}"

# TODO make dataset if necessary?
# bq --location=EU mk --dataset ${BQ_PROJECTID}:${BQ_DATASET}

if [ $USE_GOOGLE_CLOUD_STORAGE = true ]; then
  echo -e "${BROWN}[*] Uploading data to Google Cloud Storage ${NC}"
  GOOGLE_CLOUD_STORAGE_LOCATION="gs://${GOOGLE_CLOUD_STORAGE_BUCKET}/${BQ_DATASET}/${BQ_TABLE}/${DATA_FILENAME}"
  gsutil cp "${DATA_FILENAME}" "${GOOGLE_CLOUD_STORAGE_LOCATION}"
  echo -e "${GREEN}[+] Data uploaded to Google Cloud Storage! ${NC}"
fi
# Maximum file size for compressed JSON is 4 GB (https://cloud.google.com/bigquery/quotas#load_jobs)

if [ $USE_TIME_PARTITIONING = true ]; then
  echo -e "${BROWN}[*] Creating time partitioned table ${NC}"
  BQ_TABLE_CREATION_COMMAND="bq mk -t --schema ${SCHEMA_FILENAME} --time_partitioning_field ${TIME_PARTITIONING_FIELD} "
  BQ_TABLE_CREATION_COMMAND+="--time_partitioning_type DAY --project_id=${BQ_PROJECTID} ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} "
  eval "${BQ_TABLE_CREATION_COMMAND}"
  echo -e "${GREEN}[+] Table created! ${NC}"
fi

echo -e "${BROWN}[*] Loading data into BigQuery table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ${NC}"

BQ_COMMAND="bq load --source_format NEWLINE_DELIMITED_JSON --ignore_unknown_values "
if [ $USE_BIGQUERY_SCHEMA_INFERENCE = true ]; then
  BQ_COMMAND+="--autodetect "
else
  BQ_COMMAND+="--schema ${SCHEMA_FILENAME} "
fi
BQ_COMMAND+="--project_id=${BQ_PROJECTID} ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} "
if [ $USE_GOOGLE_CLOUD_STORAGE = true ]; then
  BQ_COMMAND+="${GOOGLE_CLOUD_STORAGE_LOCATION}"
else
  BQ_COMMAND+="${DATA_FILENAME}"
fi
eval "${BQ_COMMAND}"

echo -e "${GREEN}[+] Data loaded into BigQuery table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ! ${NC}"

if [ $USE_GOOGLE_CLOUD_STORAGE = true ]; then
  echo -e "${BROWN}[*] Deleting data from Google Cloud Storage ${NC}"
  gsutil rm "${GOOGLE_CLOUD_STORAGE_LOCATION}"
  echo -e "${GREEN}[+] Data deleted from Google Cloud Storage! ${NC}"
fi

if [ $DELETE_FILE = true ]; then
  echo -e "${BROWN}[*] Deleting data ${NC}"
  rm "${DATA_FILENAME}"
#  if [ $USE_BIGQUERY_SCHEMA_INFERENCE = false ]; then
#    rm "${SCHEMA_FILENAME}"
#  fi
  echo -e "${GREEN}[*] Data deleted! ${NC}"
fi

# TODO save timestamp/ObjectID of last Mongo record? for incremental upload

#  https://medium.com/google-cloud/export-load-job-with-mongodb-bigquery-part-i-64a00eb5266b
#  https://hevodata.com/blog/mongodb-to-bigquery-etl-stream-data/
#  https://stackoverflow.com/questions/42167543/mongodb-to-bigquery
