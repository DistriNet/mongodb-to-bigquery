# requires: Python package `bigquery-schema-generator` in `venv`; `google-cloud-sdk` in $PATH

RED='\033[0;31m'
GREEN='\033[0;32m'
BROWN='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

DELETE_FILE=false
USE_LOCAL_FILE=false
LOCAL_FILE=
DATA_DIR="."
USE_GOOGLE_CLOUD_STORAGE=false
GOOGLE_CLOUD_STORAGE_BUCKET=
USE_BIGQUERY_SCHEMA_INFERENCE=false
USE_LOCAL_SCHEMA_INFERENCE=false
USE_LOCAL_SCHEMA_FILE=false
LOCAL_SCHEMA_FILE=
USE_TIME_PARTITIONING=false
TEST_MODE=false
ILLEGAL_CHAR_REPLACEMENT="_"
SANITIZE_WITH_JQ=false

help () {
  printf "* Transfer MongoDB data to BigQuery *\n"
  printf "Operands:\n"
  printf "\tmongodb-to-bigquery.sh [OPTIONS] MONGODB_URI MONGODB_COLLECTION PROJECTID DATASET TABLE\n"
  printf "Options:\n"

  printf "\t-d/--data-file <file>\t\t\tUse a local JSON file instead of retrieving data from MongoDB\n"
  printf "\t-r/--data-dir <dir>\t\t\tDirectory to store (temporary) data file\n"
  printf "\t* Limit data retrieval from MongoDB:
  \t    -q/--query-file <file>\t\tUse query in provided file
  \t    -i/--incremental-id <id>\t\tOnly retrieve records after the given ObjectID
  \t    -t/--incremental-time <timestamp>\tOnly retrieve records created after the given timestamp since epoch\n"
  printf "\t* Limit field retrieval from MongoDB:
  \t    -f/--fields <fields>\t\tFields to include in the export
  \t    --field-file <file>\t\t\tFile with fields to include in the export (1 field per line)\n"
  printf "\t* Schema definition:
  \t    -b/--infer-schema-bigquery\t\tLet BigQuery infer schema (on a sample of 100)
  \t    -l/--infer-schema-local\t\tInfer schema locally (on full dataset)
  \t    -s/--schema-file <file>\t\tUse schema in provided file\n"
  printf "\t-c/--google-cloud-storage <bucket>\tStage data in given Google Cloud Storage bucket before loading into BigQuery\n"
  printf "\t-p/--time-partitioning <field>\t\tSet time partioning on given field\n"
  printf "\t--illegal-char-replacement <char>\tCharacter to replace illegal characters with. Replacement must be letter, number or underscore\n"
  printf "\t-z/--sanitize\t\t\t\tUse \`jq\` to sanitize column names\n"
  printf "Example:\n"
  printf "\tmongodb-to-bigquery.sh \"mongodb://user:pass@localhost:27017/db\" mycollection myproject mydataset mytable\n"
}
die() { echo -e "$*" 1>&2 ; exit 1; }

###################################### Option parsing ######################################
while :; do # https://unix.stackexchange.com/a/331530 http://mywiki.wooledge.org/BashFAQ/035
  case $1 in
  -h | --help)
    help
    exit 0;
    ;;
  --delete)
    DELETE_FILE=true
    ;;
  -d | --data-file)
    USE_LOCAL_FILE=true
    if [ "$2" ]; then
      LOCAL_FILE=$2
      shift
    else
      die 'ERROR: "--data-file" requires a non-empty option argument.'
    fi
    ;;
  -r | --data-dir)
    if [ "$2" ]; then
      DATA_DIR=$2
      shift
    else
      die 'ERROR: "--data-dir" requires a non-empty option argument.'
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
  -f | --fields)
    USE_FIELDS=true
    if [ "$2" ]; then
      FIELDS=$2
      shift
    else
      die 'ERROR: "--fields" requires a non-empty option argument.'
    fi
    ;;
  --field-file)
    USE_FIELD_FILE=true
    if [ "$2" ]; then
      FIELD_FILE=$2
      shift
    else
      die 'ERROR: "--field-file" requires a non-empty option argument.'
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
  --illegal-char-replacement)
    if [ "$2" ]; then
      ILLEGAL_CHAR_REPLACEMENT=$2
      shift
    else
      die 'ERROR: "--illegal-char-replacement" requires a non-empty option argument.'
    fi
    ;;
  -z | --sanitize)
    SANITIZE_WITH_JQ=true
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

BQ_LOCATION="europe-west2"

# `walk` backported from jq 1.6
# https://github.com/stedolan/jq/issues/963#issuecomment-152783116
JQ_WALK_FUNCTION='def walk(f):
  . as $in
  | if type == "object" then
      reduce keys[] as $key
        ( {}; . + { ($key):  ($in[$key] | walk(f)) } ) | f
  elif type == "array" then map( walk(f) ) | f
  else f
  end;'

if [ "${USE_LOCAL_FILE}" = true ]; then
  DATA_FILENAME="${LOCAL_FILE}"
else
  DATA_FILENAME="${DATA_DIR}/$(echo "${MONGO_COLLECTION}" | md5sum | cut -f1 -d' ').json.gz"
fi
if [ "${USE_LOCAL_SCHEMA_FILE}" = true ]; then
  SCHEMA_FILENAME="${LOCAL_SCHEMA_FILE}"
else
  SCHEMA_FILENAME="${DATA_DIR}/$(echo "${MONGO_COLLECTION}" | md5sum | cut -f1 -d' ').schema.json"
fi

echo $DATA_FILENAME $SCHEMA_FILENAME

if [ "${USE_LOCAL_FILE}" = false ]; then
  echo -e "${BROWN}[*] Retrieving data from MongoDB collection=${MONGO_COLLECTION} ${NC}"
  MONGO_COMMAND="mongoexport --uri=${MONGO_URI} --collection=${MONGO_COLLECTION} --type=json "
  QUERY_STRING=""
  if [ "${USE_QUERY_FILE}" = true ]; then
    QUERY_STRING="--query='$(cat "${QUERY_FILE}")' "
  elif [ "${USE_START_ID}" = true ]; then
    QUERY_STRING="--query='{ \"_id\": { \"\$gte\": ObjectId(\"${START_ID}\") } }' "
  elif [ "${USE_START_TIME}" = true ]; then
    QUERY_STRING="--query='{ \"_id\": { \"\$gte\": ObjectId(\"$(printf '%x\n' "${START_TIME}")0000000000000000\") } }' "
    # https://stackoverflow.com/a/8753670/7391782
  fi
  MONGO_COMMAND+=${QUERY_STRING}
  if [ "${USE_FIELDS}" = true ]; then
    MONGO_COMMAND+="--fields='${FIELDS}' "
  fi
  if [ "${USE_FIELD_FILE}" = true ]; then
    MONGO_COMMAND+="--fieldFile=${FIELD_FILE} "
  fi
  if [ "${TEST_MODE}" = true ]; then
    MONGO_COMMAND+="--limit 10000 "
  fi
  # BigQuery only accepts column names shorter than 128 characters with letters, numbers or underscores
  #  https://cloud.google.com/bigquery/docs/schemas#column_names
  # MongoDB doesn't accept signs and dots in field names, these are replaced by the code point strings '\u0024' and '\u002e' respectively
  # Replace these MongoDB substitutions + unnecessary `$date` and `$oid`
  MONGO_COMMAND+=" | sed 's/{\"\$date\":\"\([^}]*\)\"}/\"\1\"/g;s/{\"\$oid\":\"\([^}]*\)\"}/\"\1\"/g;s/\\\\u0024/${ILLEGAL_CHAR_REPLACEMENT}/g;s/\\\\u002e/${ILLEGAL_CHAR_REPLACEMENT}/g;'"

  # Use perl to replace illegal characters in and truncate keys. (faster than `jq`)
  # https://stackoverflow.com/questions/40397220/regex-substitute-character-in-a-matching-substring
  # alt: https://stackoverflow.com/questions/44536133/replace-characters-inside-a-regex-match
  MONGO_COMMAND+=' | perl -pe '\''s/(?:\G(?!\A)|\")(?=[^\"]*\":)[A-Za-z0-9_]*\K[^a-zA-Z0-9_\"]/'"${ILLEGAL_CHAR_REPLACEMENT}"'/g'\'''
  MONGO_COMMAND+=' | perl -pe '\''s/\"([^\"]{0,128})[^\"]*\"\:/\"$1\"\:/g'\''' # truncate

  if [ "${SANITIZE_WITH_JQ}" = true]; then
    # Use `jq` to replace illegal characters in and truncate keys.
    # Optional, as it is very slow. (jq seems to compile the script, so no performance loss by including the walk function)
    # https://stedolan.github.io/jq/manual/#walk(f) https://stackoverflow.com/a/42355383/7391782
    MONGO_COMMAND+=" | jq -c '${JQ_WALK_FUNCTION} walk(if type == \"object\" then with_entries(.key |= gsub(\"[^a-zA-Z0-9_]\";\"${ILLEGAL_CHAR_REPLACEMENT}\")[0:128]) else . end )'"
  fi
  # gzip to reduce data size
  MONGO_COMMAND+=" | gzip"
  # TODO gzip to disk for space, but upload uncompressed for faster processing?
  # https://cloud.google.com/bigquery/docs/loading-data#loading_compressed_and_uncompressed_data
  # https://www.oreilly.com/library/view/google-bigquery-the/9781492044451/ch04.html
  MONGO_COMMAND+="> ${DATA_FILENAME}"
  eval "$MONGO_COMMAND" || die "${RED}[-] Failed to retrieve data from MongoDB! ${NC}"
  LAST_RECORD=$(mongoexport --uri="${MONGO_URI}" --collection="${MONGO_COLLECTION}" --type json "${QUERY_STRING}" --sort='{_id:-1}' --limit=1 2>/dev/null | jq -r '._id."$oid"')
  LAST_TIMESTAMP=$(date +%s)
  echo -e "${CYAN}[>] Last record: ${LAST_RECORD} ; timestamp: ${LAST_TIMESTAMP} ${NC}"
else
  if [ ! -f "${DATA_FILENAME}" ]; then
    die "${RED}[-] Data file does not exist ${NC}"
  fi
  echo -e "${BROWN}[*] Reading data from local file ${DATA_FILENAME} ${NC}"
fi
echo -e "${GREEN}[+] Data retrieved successfully! ${NC}"

# TODO split file into smaller chunks? query in smaller chunks?

# TODO preprocess JSON to remove invalid keys; keys that only differ in case https://cloud.google.com/bigquery/docs/schemas#column_names
# won't be solved by schema generator: https://github.com/bxparks/bigquery-schema-generator/issues/39
# TODO  --sanitize_names ? (this won't be applied to the data though?)

if [ $USE_LOCAL_SCHEMA_INFERENCE = true ]; then
  # infer BigQuery schema across whole file (not 100 record sample) using https://pypi.org/project/bigquery-schema-generator/
  echo -e "${BROWN}[*] Generating BigQuery schema ${NC}"
  zcat "${DATA_FILENAME}" | venv/bin/generate-schema >"${SCHEMA_FILENAME}" || die "${RED}[-] Failed to generate schema! ${NC}"
  # TODO `head` to make local inference faster?
fi
if [ ! -f "${SCHEMA_FILENAME}" ] && [ "${USE_BIGQUERY_SCHEMA_INFERENCE}" = false ]; then
  die "${RED}[-] Schema file does not exist ${NC}"
fi
echo -e "${GREEN}[+] BigQuery schema available! ${NC}"

if [ $USE_GOOGLE_CLOUD_STORAGE = true ]; then
  echo -e "${BROWN}[*] Uploading data to Google Cloud Storage ${NC}"
  GOOGLE_CLOUD_STORAGE_LOCATION="gs://${GOOGLE_CLOUD_STORAGE_BUCKET}/${BQ_DATASET}/${BQ_TABLE}/$(basename "${DATA_FILENAME}")"
  gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp "${DATA_FILENAME}" "${GOOGLE_CLOUD_STORAGE_LOCATION}" || die "${RED}[-] Failed to upload data! ${NC}"
  echo -e "${GREEN}[+] Data uploaded to Google Cloud Storage! ${NC}"
fi
# Maximum file size for compressed JSON is 4 GB (https://cloud.google.com/bigquery/quotas#load_jobs)

# Make dataset if necessary
# TODO Verify? Dataset IDs must be alphanumeric (plus underscores) and must be at most 1024 characters long.
# TODO - or do before everything else
if ! bq show "${BQ_PROJECTID}":"${BQ_DATASET}" > /dev/null; then
  echo -e "${BROWN}[*] Creating BigQuery dataset ${BQ_PROJECTID}:${BQ_DATASET} ${NC}"
  bq --location=${BQ_LOCATION} mk --dataset "${BQ_PROJECTID}":"${BQ_DATASET}" || die "${RED}[-] Failed to create dataset! ${NC}"
  echo -e "${GREEN}[+] Created dataset ${BQ_PROJECTID}:${BQ_DATASET} ! ${NC}"
fi

if [ $USE_TIME_PARTITIONING = true ]; then
  echo -e "${BROWN}[*] Creating time partitioned table ${NC}"
  BQ_TABLE_CREATION_COMMAND="bq mk -t --schema ${SCHEMA_FILENAME} --time_partitioning_field ${TIME_PARTITIONING_FIELD} "
  BQ_TABLE_CREATION_COMMAND+="--time_partitioning_type DAY --project_id=${BQ_PROJECTID} ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} "
  eval "${BQ_TABLE_CREATION_COMMAND}" || die "${RED}[-] Failed to create table! ${NC}"
  echo -e "${GREEN}[+] Table created! ${NC}"
fi

echo -e "${BROWN}[*] Loading data into BigQuery table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ${NC}"

BQ_COMMAND="bq --location=${BQ_LOCATION} load --source_format NEWLINE_DELIMITED_JSON --ignore_unknown_values "
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
if eval "${BQ_COMMAND}"; then
  echo -e "${GREEN}[+] Data loaded into BigQuery table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ! ${NC}"
else
  die "${RED}[-] Failed to load data into BigQuery table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ! ${NC}"
fi

if [ $USE_GOOGLE_CLOUD_STORAGE = true ]; then
  echo -e "${BROWN}[*] Deleting data from Google Cloud Storage ${NC}"
  if gsutil rm "${GOOGLE_CLOUD_STORAGE_LOCATION}"; then
    echo -e "${GREEN}[+] Data deleted from Google Cloud Storage! ${NC}"
  else
    echo -e "${RED}[-] Failed to delete data from Google Cloud Storage! ${NC}"
  fi
fi

if [ $DELETE_FILE = true ]; then
  echo -e "${BROWN}[*] Deleting data ${NC}"
  if rm "${DATA_FILENAME}"; then
    echo -e "${GREEN}[+] Data deleted! ${NC}"
  else
    echo -e "${RED}[-] Failed to delete data! ${NC}"
  fi
#  if [ $USE_BIGQUERY_SCHEMA_INFERENCE = false ]; then
#    rm "${SCHEMA_FILENAME}"
#  fi
fi

#  https://medium.com/google-cloud/export-load-job-with-mongodb-bigquery-part-i-64a00eb5266b
#  https://hevodata.com/blog/mongodb-to-bigquery-etl-stream-data/
#  https://stackoverflow.com/questions/42167543/mongodb-to-bigquery
