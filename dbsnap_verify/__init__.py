from time import time

import datetime

import json

import boto3
s3 = boto3.client("s3")

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

secs_in_day = 86400

def now_timestamp():
    return int(time())

def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp)

def now_datetime():
    return datetime.datetime.now()

def today_date():
    return now_datetime()

def tomorrow_date():
    return(now_datetime() + datetime.timedelta(days=1))

def datetime_to_date_str(dt):
    return dt.strftime("%Y-%m-%d")

def date_str_to_datetime(date_str):
    return datetime.datetime(*map(int, date_str.split("-")))

def get_snapshot_description(event):
    """if it exists return the snapshot definition, else None"""
    return None

def get_database_description(event):
    """if it exists return the database definition, else None"""
    return None

def start_restore_from_snapshot(event):
    pass

def wait(state_doc):
    logger.info("Looking for the {snapshot_date} snapshot of {database}".format(**state_doc))
    description = get_snapshot_description(event)
    if description:
        restore(event, state_doc)
    elif today_date() > date_str_to_datetime(state_doc["snapshot_date"]):
        logger.warning("Alert! we never found the {snapshot_date} snapshot for {database}".format(**state_doc))
        alarm("asdfasdfasdkfjnasdf naskdfn aksdjfn")
    else:
        set_state_doc_in_s3(state_doc, "wait")
        logger.info("Did not find the {snapshot_date} snapshot of {database}".format(**state_doc))
        logger.info("Going to sleep.")

def restore(state_doc):
    description = get_database_descripton(state_doc)
    if description is None:
        set_state_doc_in_s3(state_doc, "restore")
        start_restore_from_snapshot(state_doc)
    elif description[DBInstanceStatus] == "available":
        set_state_doc_in_s3("verify")
        verify(state_doc, description)
        
def verify(state_doc, description):
    reset_master_password()
    connection = connect_to_endpoint(description["endpoint"])
    result = run_all_the_tests(connection, state_doc["verfication_checks"])
    if result:
        set_state_doc_in_s3("success")
        alarm(state_doc, "success")
    else:
        set_state_doc_in_s3("alarm")
        alarm(state_doc, "error")
    set_state_doc_in_s3("cleanup")
    clean_up(state_doc)

def cleanup(state_doc):
    description = get_database_descripton(state_doc)
    if description is None:
        # start waiting for tomorrows date.
        set_state_doc_in_s3("wait")
    elif description[DBInstanceStatus] == "available":
        destroy_database(state_doc)

def alarm(state_doc):
    # trigger an alarm, maybe cloudwatch or something.
    pass

def upload_state_doc(state_doc):
    state_doc_json = json.dumps(state_doc)
    s3.put_object(
        Bucket=state_doc["state_dot_bucket"],
        Key=state_doc_s3_key(event["database"]),
        Body=state_doc_json,
    )

def set_state_doc_in_s3(state_doc, new_state):
    if "states" not in state_doc:
        state_doc["states"] = []
    state_doc["states"].append(
        {
            "state" : new_state,
            "timestamp" : now_timestamp(),
        }
    )
    upload_state_doc(state_doc)
    return state_doc

def state_doc_s3_key(database):
    return "state-doc-{}.json".format(database)

def download_state_doc(event):
    s3_object = s3.get_object(
        Bucket=event["state_doc_bucket"],
        Key=state_doc_s3_key(event["database"]),
    )
    # download state_doc json from s3 and stick it into a string.
    state_doc_json = s3_object["Body"].read()
    # turn json into a dict and return it.
    return json.loads(state_doc_json)

def get_or_create_state_doc_in_s3(event):
    """get (or create if missing) the state_doc in S3."""
    try:
        state_doc = download_state_doc(event)
    except s3.exceptions.NoSuchKey:
        state_doc = event
        state_doc["snapshot_date"] = datetime_to_date_str(tomorrow_date())
        state_doc = set_state_doc_in_s3(state_doc, "wait")
    return state_doc

def current_state(state_doc):
    return state_doc["states"][-1]["state"]

state_handlers = {
  "wait": wait,
  "restore": restore,
  "verify": verify,
  "cleanup": cleanup,
  "alarm": alarm,
}

def lambda_handler(event, context):
    """The main entrypoint called when our AWS Lambda wakes up."""
    event = json.loads(event)
    state_doc = get_or_create_state_doc_in_s3(event)
    state_handler = state_handlers[current_state(state_doc)]
    state_handler(state_doc)