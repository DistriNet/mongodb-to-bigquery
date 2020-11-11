# requires: Python package `bigquery-schema-generator` in `venv`; `google-cloud-sdk` in $PATH
{ # Ensure script is ingested/executed as a whole
  # https://unix.stackexchange.com/questions/331837/how-to-read-the-whole-shell-script-before-executing-it
RED='\033[0;31m'
GREEN='\033[0;32m'
BROWN='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

help() {
  printf "* Transfer MongoDB data to BigQuery *\n"
  printf "Operands:\n"
  printf "\tmongodb-to-bigquery.sh [OPTIONS] MONGODB_URI MONGODB_COLLECTION PROJECTID DATASET TABLE\n"
  printf "Options:\n"

  printf "\t-d/--data-file <file>\t\t\tUse a local JSONL file instead of retrieving data from MongoDB\n"
  printf "\t-r/--data-dir <dir>\t\t\tDirectory to store (temporary) data file\n"
  printf "\t* Limit data retrieval from MongoDB:
  \t    -q/--query-file <file>\t\tUse query in provided file
  \t    --start-id <id>\t\tOnly retrieve records after the given ObjectID
  \t    --start-time <timestamp>\tOnly retrieve records created after the given timestamp since epoch
  \t    --end-id <id>\t\tOnly retrieve records before the given ObjectID
  \t    --end-time <timestamp>\tOnly retrieve records created before the given timestamp since epoch
  \t    (Supported combination: (query-file) and/or (start-id or start-time) and/or (end-id or end-id))\n"
  printf "\t* Limit field retrieval from MongoDB:
  \t    -f/--fields <fields>\t\tFields to include in the export
  printf "\t* Schema definition:
  \t    -b/--infer-schema-bigquery\t\tLet BigQuery infer schema (on a sample of 100)
  \t    -l/--infer-schema-local\t\tInfer schema locally (on full dataset)
  \t    -s/--schema-file <file>\t\tUse schema in provided file\n"
  printf "\t-c/--google-cloud-storage <bucket>\tStage data in given Google Cloud Storage bucket before loading into BigQuery\n"
  printf "\t-p/--time-partitioning <field>\t\tSet time partioning on given field\n"
  printf "\t--illegal-char-replacement <char>\tCharacter to replace illegal characters with. Replacement must be letter, number or underscore\n"
  printf "\t* Sanitization (comply with BigQuery column name requirements):
  \t    -z/--sanitize-with-regex\t\tUse regex to sanitize column names
  \t    --sanitize-with-jq\t\t\tUse \`jq\` to sanitize column names\n"
  printf "\t-a/--allow-bad-records\t\t\tSkip bad records when loading data into BigQuery\n"
  printf "\t-e/--remove-table-if-exists\t\t\tRemove the table if it already exists\n"
  printf "Examples:\n"
  printf "\t${0##*/} \"mongodb://user:pass@localhost:27017/db\" mycollection myproject mydataset mytable\n"
  printf "\t${0##*/} --data-file my_data.json -b myproject mydataset mytable\n"
}
die() {
  echo -e "$*" 1>&2
  exit 1
}

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
FILTER_WITH_JQ=false
JQ_FILTER=
SANITIZE_WITH_REGEX=false
SANITIZE_WITH_JQ=false
BIGQUERY_ALLOW_BAD_RECORDS=false
REMOVE_TABLE_IF_EXISTS=false

###################################### Option parsing ######################################
while :; do # https://unix.stackexchange.com/a/331530 http://mywiki.wooledge.org/BashFAQ/035
  case $1 in
  -h | --help)
    help
    exit 0
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
  -q | --query-file)
    USE_QUERY_FILE=true
    if [ "$2" ]; then
      QUERY_FILE=$2
      shift
    else
      die 'ERROR: "--query-file" requires a non-empty option argument.'
    fi
    ;;
  --start-id)
    USE_START_ID=true
    if [ "$2" ]; then
      START_ID=$2
      shift
    else
      die 'ERROR: "--start-id" requires a non-empty option argument.'
    fi
    ;;
  --start-time)
    USE_START_TIME=true
    if [ "$2" ]; then
      START_TIME=$2
      shift
    else
      die 'ERROR: "--start-time" requires a non-empty option argument.'
    fi
    ;;
  --end-id)
    USE_END_ID=true
    if [ "$2" ]; then
      END_ID=$2
      shift
    else
      die 'ERROR: "--end-id" requires a non-empty option argument.'
    fi
    ;;
  --end-time)
    USE_END_TIME=true
    if [ "$2" ]; then
      END_TIME=$2
      shift
    else
      die 'ERROR: "--end-time" requires a non-empty option argument.'
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
  -j | --filter-with-jq)
    FILTER_WITH_JQ=true
    if [ "$2" ]; then
      JQ_FILTER=$2
      shift
    else
      die 'ERROR: "--filter-with-jq" requires a non-empty option argument.'
    fi
    ;;
  -z | --sanitize-with-regex)
    SANITIZE_WITH_REGEX=true
    ;;
  --sanitize-with-jq)
    SANITIZE_WITH_JQ=true
    ;;
  -a | --allow-bad-records)
    BIGQUERY_ALLOW_BAD_RECORDS=true
    ;;
  -e | --remove-table-if-exists)
    REMOVE_TABLE_IF_EXISTS=true
    ;;
  --test)
    TEST_MODE=true
    ;;
  *) break ;;
  esac
  shift
done
############################################################################################

if [ "${USE_LOCAL_FILE}" = true ]; then
  if [ "$#" -ne 3 ]; then
    echo -e "${RED}Missing parameters.${NC}"
    help
    exit 1
  fi

  BQ_PROJECTID=$1
  BQ_DATASET=$2
  BQ_TABLE=$3
else
  if [ "$#" -ne 5 ]; then
    echo -e "${RED}Missing parameters.${NC}"
    help
    exit 1
  fi

  MONGO_URI=$1
  MONGO_COLLECTION=$2
  BQ_PROJECTID=$3
  BQ_DATASET=$4
  BQ_TABLE=$5
fi

BQ_LOCATION="US"
SUFFIX=".json.gz"

# TODO improve file handling with suffix

if [ "${USE_LOCAL_FILE}" = true ]; then
  DATA_FILENAME="${DATA_DIR}/${LOCAL_FILE}"
  DATA_FILE_GLOB="${DATA_FILENAME}"
  BASENAME=$(basename "${DATA_FILE_GLOB}")
else
  DATA_FILENAME="${DATA_DIR}/$(echo "${MONGO_COLLECTION}" | md5sum | cut -f1 -d' ')${SUFFIX}"
  DATA_FILE_GLOB="$(echo "${DATA_FILENAME}" | cut -f 1 -d '.')*${SUFFIX}"
  BASENAME=$(basename "${DATA_FILE_GLOB}" ${SUFFIX})
fi
if [ "${USE_LOCAL_SCHEMA_FILE}" = true ]; then
  SCHEMA_FILENAME="${LOCAL_SCHEMA_FILE}"
else
  if [ "${USE_LOCAL_FILE}" = true ]; then
    SCHEMA_FILENAME="${DATA_DIR}/$(echo "${LOCAL_FILE}" | md5sum | cut -f1 -d' ').schema.json"
  else
    SCHEMA_FILENAME="${DATA_DIR}/$(echo "${MONGO_COLLECTION}" | md5sum | cut -f1 -d' ').schema.json"
  fi
fi

if [ "${USE_LOCAL_FILE}" = false ]; then
  echo -e "${BROWN}[*] Retrieving data from MongoDB collection=${MONGO_COLLECTION} ${NC}"
  MONGO_COMMAND="mongoexport --uri=${MONGO_URI} --collection=${MONGO_COLLECTION} --type=json "

  if [ "${USE_QUERY_FILE}" = true ] || [ "${USE_START_ID}" = true ] || [ "${USE_START_TIME}" = true ] || [ "${USE_END_ID}" = true ] || [ "${USE_END_TIME}" = true ]; then
    # Build MongoDB filter query
    if [ "${USE_START_TIME}" = true ]; then
      # https://stackoverflow.com/a/8753670/7391782
      START_ID=$(printf '%x\n' "${START_TIME}")0000000000000000
    fi
    if [ "${USE_START_ID}" = true ] || [ "${USE_START_TIME}" = true ]; then
      QUERY_STRING="--query='{ \"_id\": { \"\$gte\": {\"\$oid\": \"${START_ID}\" } } }' "
    fi
    if [ "${USE_END_TIME}" = true ]; then
      # https://stackoverflow.com/a/8753670/7391782
      END_ID=$(printf '%x\n' "${END_TIME}")0000000000000000
    fi
    if [ "${USE_END_ID}" = true ] || [ "${USE_END_TIME}" = true ]; then
      QUERY_STRING="--query='{ \"_id\": { \"\$gte\": {\"\$oid\": \"${END_ID}\" } } }' "
    fi

    JQ_COMMAND_ARG=""
    JQ_COMMAND_FILTER=""
    if [ "${USE_START_ID}" = true ] || [ "${USE_START_TIME}" = true ] || [ "${USE_END_ID}" = true ] || [ "${USE_END_TIME}" = true ]; then
      # should be fine to have both $gte and $lte directly (i.e. not combined with $and)
      JQ_COMMAND_FILTER+="\"_id\": {"
      if [ "${USE_START_ID}" = true ] || [ "${USE_START_TIME}" = true ]; then
        JQ_COMMAND_ARG+=" --arg startid ${START_ID}"
        # shellcheck disable=SC2016
        JQ_COMMAND_FILTER+='"$gte": {"$oid": $startid } , '  # single quotes to preserve $ for jq
      fi
      if [ "${USE_END_ID}" = true ] || [ "${USE_END_TIME}" = true ]; then
        JQ_COMMAND_ARG+=" --arg endid ${END_ID}"  # naming it 'end' gives errors https://github.com/stedolan/jq/issues/1619
        # shellcheck disable=SC2016
        JQ_COMMAND_FILTER+='"$lte": {"$oid": $endid } , '  # single quotes to preserve $ for jq
      fi
      JQ_COMMAND_FILTER+="}"
    fi
    if [ "${USE_QUERY_FILE}" = true ]; then
      JQ_COMMAND_FILE=${QUERY_FILE}
    else
      JQ_COMMAND_FILE="-n"
    fi

    JQ_COMMAND="jq -c ${JQ_COMMAND_ARG} '. + {${JQ_COMMAND_FILTER}}' ${JQ_COMMAND_FILE}"
    JQ_RESULT=$(eval "${JQ_COMMAND}")
    MONGO_COMMAND+="--query='${JQ_RESULT}' "
  fi

  if [ "${USE_FIELDS}" = true ]; then
    MONGO_COMMAND+="--fields='${FIELDS}' "
  fi
  if [ "${TEST_MODE}" = true ]; then
    MONGO_COMMAND+="--limit 10000 "
  fi

  if [ "${FILTER_WITH_JQ}" = true ]; then
    MONGO_COMMAND+=" | jq -c ${JQ_FILTER}"
  fi

  # Replace unnecessary `$date` and `$oid`
  MONGO_COMMAND+=" | sed 's/{\"\$date\":\"\([^}]*\)\"}/\"\1\"/g;s/{\"\$oid\":\"\([^}]*\)\"}/\"\1\"/g;'"

  if [ "${SANITIZE_WITH_REGEX}" = true ]; then
    # Use if you aren't sure that column names are according to BigQuery requirements.
    # BigQuery only accepts column names shorter than 128 characters with letters, numbers or underscores,
    #  starting with a letter or underscore
    #  https://cloud.google.com/bigquery/docs/schemas#column_names

    # MongoDB doesn't accept signs and dots in field names, these are replaced by the code point strings '\u0024' and '\u002e' respectively
    # Use sed to replace these MongoDB substitutions
    MONGO_COMMAND+=" | sed 's/\\\\u0024/${ILLEGAL_CHAR_REPLACEMENT}/g;s/\\\\u002e/${ILLEGAL_CHAR_REPLACEMENT}/g;'"
    # Use perl to replace illegal characters in and truncate keys. (faster than `jq`)
    # https://stackoverflow.com/questions/40397220/regex-substitute-character-in-a-matching-substring
    # alt: https://stackoverflow.com/questions/44536133/replace-characters-inside-a-regex-match
    MONGO_COMMAND+=' | perl -pe '\''s/(?:\G(?!\A)|[,{]\")(?=[^\"]+\":)[A-Za-z0-9_]*\K[^a-zA-Z0-9_\"]/'"${ILLEGAL_CHAR_REPLACEMENT}"'/g'\'''
    # shellcheck disable=SC2016
    MONGO_COMMAND+=' | perl -pe '\''s/(?<=[,{]\")([^a-zA-Z_][^\"]*)(?=\"\:)/'"${ILLEGAL_CHAR_REPLACEMENT}"'$1/g'\''' # start with valid character
    # shellcheck disable=SC2016
    MONGO_COMMAND+=' | perl -pe '\''s/(?<=[,{]\")([^\"]{1,128}+)(?=[^\"]*\"\:)/$1/g'\''' # truncate
    # Multiple perl processes -> form of parallelization
    # TODO A column name cannot use any of the following prefixes: _TABLE_ _FILE_ _PARTITION
    # TODO what to do about "Duplicate column names are not allowed even if the case differs"?
    # TODO address escaped double quotes in keys
  fi
  if [ "${SANITIZE_WITH_JQ}" = true ]; then
    # Use `jq` to replace illegal characters in and truncate keys.
    # Optional, as it is very slow. (jq seems to compile the script, so no performance loss by including the walk function)

    # https://stedolan.github.io/jq/manual/#walk(f) https://stackoverflow.com/a/42355383/7391782
    # `walk` backported from jq 1.6
    # https://github.com/stedolan/jq/issues/963#issuecomment-152783116
    # shellcheck disable=SC2016
    JQ_WALK_FUNCTION='def walk(f):
      . as $in
      | if type == "object" then
          reduce keys[] as $key
            ( {}; . + { ($key):  ($in[$key] | walk(f)) } ) | f
      elif type == "array" then map( walk(f) ) | f
      else f
      end;'

    MONGO_COMMAND+=" | jq -c '${JQ_WALK_FUNCTION} walk(if type == \"object\" then with_entries(.key |= gsub(\"[^a-zA-Z0-9_]\";\"${ILLEGAL_CHAR_REPLACEMENT}\")[0:128]) else . end )'"
  fi

  # TODO make split optional?
  if [ true ]; then
    # gzip (in parallel = pigz) to reduce data size
    # gzip files must be smaller than 4G - split while preserving newlines
    # https://stackoverflow.com/questions/47062749/most-efficient-way-to-split-a-compressed-csv-into-chunks
    MONGO_COMMAND+=" | split -C 4G -d - $(echo "${DATA_FILENAME}" | cut -f 1 -d '.') --filter 'pigz > "'$FILE'"${SUFFIX}'"
    # TODO gzip to disk for space, but upload uncompressed for faster processing?
    # https://cloud.google.com/bigquery/docs/loading-data#loading_compressed_and_uncompressed_data
    # https://www.oreilly.com/library/view/google-bigquery-the/9781492044451/ch04.html
  else
    MONGO_COMMAND+="> ${DATA_FILENAME}"
  fi

  eval "$MONGO_COMMAND" || die "${RED}[-] Failed to retrieve data from MongoDB! ${NC}"
  LAST_RECORD=$(mongoexport --uri="${MONGO_URI}" --collection="${MONGO_COLLECTION}" --type json "${QUERY_STRING}" --sort='{_id:-1}' --limit=1 2>/dev/null | jq -r '._id."$oid"')
  LAST_TIMESTAMP=$(date +%s)
  echo -e "${CYAN}[>] Last record: ${LAST_RECORD} ; timestamp: ${LAST_TIMESTAMP} ${NC}"
else
  if ! ls "${DATA_FILE_GLOB}" 1> /dev/null 2>&1; then
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
  # no quotes for ls: we WANT globbing
  # shellcheck disable=SC2086
  ls -1 ${DATA_FILE_GLOB} | xargs pigz -dc | venv/bin/generate-schema >"${SCHEMA_FILENAME}" || die "${RED}[-] Failed to generate schema! ${NC}"
  # TODO add --ignore_invalid_lines once update to generator is released
fi
if [ ! -s "${SCHEMA_FILENAME}" ] && [ "${USE_BIGQUERY_SCHEMA_INFERENCE}" = false ]; then
  die "${RED}[-] Schema file does not exist ${NC}"
fi
echo -e "${GREEN}[+] BigQuery schema available! ${NC}"

if [ $USE_GOOGLE_CLOUD_STORAGE = true ]; then
  echo -e "${BROWN}[*] Uploading data to Google Cloud Storage ${NC}"
  GOOGLE_CLOUD_STORAGE_LOCATION="gs://${GOOGLE_CLOUD_STORAGE_BUCKET}/${BQ_DATASET}/${BQ_TABLE}"

  gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m cp ${DATA_FILE_GLOB} "${GOOGLE_CLOUD_STORAGE_LOCATION}/" || die "${RED}[-] Failed to upload data! ${NC}"
  echo -e "${GREEN}[+] Data uploaded to Google Cloud Storage! ${NC}"
fi
# Maximum file size for compressed JSON is 4 GB (https://cloud.google.com/bigquery/quotas#load_jobs)

# Make dataset if necessary
# TODO Verify? Dataset IDs must be alphanumeric (plus underscores) and must be at most 1024 characters long.
# TODO - or do before everything else
if ! bq show "${BQ_PROJECTID}":"${BQ_DATASET}" >/dev/null; then
  echo -e "${BROWN}[*] Creating BigQuery dataset ${BQ_PROJECTID}:${BQ_DATASET} ${NC}"
  bq --location=${BQ_LOCATION} mk --dataset "${BQ_PROJECTID}":"${BQ_DATASET}" || die "${RED}[-] Failed to create dataset! ${NC}"
  echo -e "${GREEN}[+] Created dataset ${BQ_PROJECTID}:${BQ_DATASET} ! ${NC}"
fi

if [ $REMOVE_TABLE_IF_EXISTS = true ]; then
  if bq show "${BQ_PROJECTID}":"${BQ_DATASET}.${BQ_TABLE}" >/dev/null; then
    echo -e "${BROWN}[*] Removing BigQuery table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ${NC}"
    bq rm -f "${BQ_PROJECTID}":"${BQ_DATASET}.${BQ_TABLE}"
    echo -e "${GREEN}[+] Removed table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ${NC}"
  fi
fi

if [ $USE_TIME_PARTITIONING = true ]; then
    if ! bq show "${BQ_PROJECTID}":"${BQ_DATASET}.${BQ_TABLE}" >/dev/null; then
      echo -e "${BROWN}[*] Creating time partitioned table ${NC}"
      BQ_TABLE_CREATION_COMMAND="bq mk -t --schema ${SCHEMA_FILENAME} --time_partitioning_field ${TIME_PARTITIONING_FIELD} "
      BQ_TABLE_CREATION_COMMAND+="--time_partitioning_type DAY --project_id=${BQ_PROJECTID} ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} "
      eval "${BQ_TABLE_CREATION_COMMAND}" || die "${RED}[-] Failed to create table! ${NC}"
      echo -e "${GREEN}[+] Table created! ${NC}"
    else
      echo -e "${BROWN}[!] Time partitioned table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} already exists. ${NC}"
    fi
fi

echo -e "${BROWN}[*] Loading data into BigQuery table ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} ${NC}"

BQ_COMMAND="bq --location=${BQ_LOCATION} load --source_format NEWLINE_DELIMITED_JSON --ignore_unknown_values "
if [ "$BIGQUERY_ALLOW_BAD_RECORDS" = true ]; then
  BQ_COMMAND+="--max_bad_records=999999999 "
fi
if [ $USE_BIGQUERY_SCHEMA_INFERENCE = true ]; then
  BQ_COMMAND+="--autodetect "
else
  BQ_COMMAND+="--schema ${SCHEMA_FILENAME} "
fi
BQ_COMMAND+="--project_id=${BQ_PROJECTID} ${BQ_PROJECTID}:${BQ_DATASET}.${BQ_TABLE} "
if [ $USE_GOOGLE_CLOUD_STORAGE = true ]; then
  BQ_COMMAND+="${GOOGLE_CLOUD_STORAGE_LOCATION}/${BASENAME}"
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
  if gsutil rm "${GOOGLE_CLOUD_STORAGE_LOCATION}/${BASENAME}"; then
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
}; exit
#  https://medium.com/google-cloud/export-load-job-with-mongodb-bigquery-part-i-64a00eb5266b
#  https://hevodata.com/blog/mongodb-to-bigquery-etl-stream-data/
#  https://stackoverflow.com/questions/42167543/mongodb-to-bigquery
