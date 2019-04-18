from os import environ

import json

import time

import boto3

from dbsnap.rds_funcs import dbsnap_verify_identifier

DB_ID_PREFIX_LEN = 5

try:
    basestring
except NameError:
    basestring = str


def now_timestamp():
    return time.time()


def timestamp_to_isoformat(ts):
    from datetime import datetime

    return datetime.utcfromtimestamp(ts).isoformat()


class DocToObject(object):
    """
    Produce a Python object from a dict or json document.
    Make document keys is accessible as object attributes.
    """

    def setattrs_from_dict(self, dictionary):
        for key, value in dictionary.items():
            try:
                setattr(self, key, value)
            except AttributeError:
                pass

    def __init__(self, document=None):
        """document (json/dictionary): a document to turn into an object."""
        if document is not None:
            self.from_json(document)

    def from_json(self, document):
        if isinstance(document, dict):
            self.setattrs_from_dict(document)
        elif isinstance(document, basestring):
            self.setattrs_from_dict(json.loads(document))
        else:
            raise Exception(
                "Invalid type ({}), must be dict or JSON basestring.".format(
                    type(document)
                )
            )

    @property
    def to_json(self):
        return json.dumps(self.__dict__, indent=2)


class StateDoc(DocToObject):
    def __init__(
        self, name, states=None, state_doc_path=None, state_doc_bucket=None, **kwargs
    ):

        if states is None:
            self.states = []
        else:
            self.states = states
        self.state_doc_name = name
        self.state_doc_path = state_doc_path
        self.state_doc_bucket = state_doc_bucket
        super(StateDoc, self).__init__(kwargs)

    @property
    def state_doc_bucket_name(self):
        return environ.get("STATE_DOC_BUCKET", self.state_doc_bucket)

    @property
    def state_doc_file_path(self):
        return environ.get("STATE_DOC_PATH", self.state_doc_path)

    @property
    def current_state(self):
        if self.states:
            return self.states[-1]["state"]

    @property
    def valid_transitions(self):
        return self.transition_map.get(self.current_state, [])

    @property
    def transition_map(self):
        """You may overload this property."""
        return {}

    @property
    def persistence(self):
        """str: "state_doc_bucket" or "state_doc_path" or raises exception."""
        if self.state_doc_bucket_name and self.state_doc_file_path:
            msg = "Choose either `state_doc_bucket` or `state_doc_path` not both."
            raise Exception(msg)
        elif self.state_doc_bucket_name:
            return "state_doc_bucket"
        elif self.state_doc_file_path:
            return "state_doc_path"

    @property
    def state_doc_s3_key(self):
        return "state-doc-{}.json".format(self.state_doc_name)

    def _save_state_doc_in_s3(self):
        if self.state_doc_bucket_name:
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=self.state_doc_bucket_name,
                Key=self.state_doc_s3_key,
                Body=self.to_json,
            )

    def _load_state_doc_from_s3(self):
        """Returns a JSON String State Document."""
        s3 = boto3.client("s3")
        s3_object = s3.get_object(
            Bucket=self.state_doc_bucket_name, Key=self.state_doc_s3_key
        )
        return s3_object["Body"].read().decode("utf-8")

    def _save_state_doc_in_path(self):
        with open(self.state_doc_file_path, "w") as json_file:
            json_file.write(self.to_json)

    def _load_state_doc_from_path(self):
        """Returns a JSON String State Document."""
        with open(self.state_doc_file_path, "r") as json_file:
            return json_file.read()

    def is_valid_transition(self, new_state):
        return new_state in self.valid_transitions

    def transition_state(self, new_state, validate=True):
        """Change the state of the StateDoc and save to persistence."""
        if validate and self.transition_map:
            if self.is_valid_transition(new_state) == False:
                raise Exception(
                    """
                    Invalid state transition from {} -> {}.
                    Valid transitions are {}.
                    """.format(
                        self.current_state, new_state, self.valid_transitions
                    )
                )
        timestamp = now_timestamp()
        self.states.append(
            {
                "state": new_state,
                "timestamp": timestamp,
                "utc_iso_format": timestamp_to_isoformat(timestamp),
            }
        )
        self.save()

    def trim_states(self, count_to_keep=100):
        trim_index = len(self.states) - count_to_keep
        self.states = self.states[trim_index:]

    def save(self):
        if self.persistence == "state_doc_bucket":
            self._save_state_doc_in_s3()
        elif self.persistence == "state_doc_path":
            self._save_state_doc_in_path()

    def load(self):
        if self.persistence == "state_doc_bucket":
            self.from_json(self._load_state_doc_from_s3())
        elif self.persistence == "state_doc_path":
            self.from_json(self._load_state_doc_from_path())


class DbsnapVerifyStateDoc(StateDoc):
    """
    This object is passed around in the state machine.
    It can persist it's state in a file path or s3.
    """

    def __init__(
        self,
        database,
        database_subnet_ids=None,
        database_security_group_ids=None,
        snapshot_region=None,
        states=None,
        state_doc_path=None,
        state_doc_bucket=None,
        snapshot_verifying=None,
        snapshot_verified=None,
        tmp_password=None,
        checks=None,
        **kwargs
    ):
        """
        database (string):
            The AWS RDS DB Identifier whose snapshot we should restore/verify.

        database_subnet_ids (string):
            A CSV of subnet ids to create a database subnet group with.

        database_security_group_ids (string):
            A CSV of security group ids to add to the newly restored
            temporary database instance.

        snapshot_region (string):
            The region to find the snapshot and restore/verify into.

        state_doc_path (string):
            The path to the local file to store the state document.
            If you choose this, do not set state_doc_bucket.

        state_doc_bucket (string):
            The S3 bucket to store the state document.
            If you choose this, do not set state_doc_path.

        tmp_password (string):
            The temporary randomly generated RDS master password.
            This is used for data verification.

        snapshot_verifying (string):
            The current AWS RDS Snapshot ID under verification.

        snapshot_verified (string):
            The most recently verified AWS RDS Snapshot ID.

        states (list):
            A list of recent state transitions.

        checks (dict):
            A dictionary of checks to verify the DB.
            This is used for data verification
        """
        super(DbsnapVerifyStateDoc, self).__init__(
            name=database,
            states=states,
            state_doc_path=state_doc_path,
            state_doc_bucket=state_doc_bucket,
            database=database,
            database_subnet_ids=database_subnet_ids,
            database_security_group_ids=database_security_group_ids,
            snapshot_region=snapshot_region,
            snapshot_verifying=snapshot_verifying,
            snapshot_verified=snapshot_verified,
            tmp_password=tmp_password,
            checks=checks,
            **kwargs
        )

    @property
    def tmp_database(self):
        return dbsnap_verify_identifier(self.database)

    def clean(self, state_count_to_keep=100):
        self.tmp_password = None
        self.snapshot_verified = self.snapshot_verifying
        self.snapshot_verifying = None
        self.trim_states(state_count_to_keep)

    def _csv_to_list(self, csv):
        if isinstance(csv, basestring):
            return csv.split(",")
        return csv

    @property
    def subnet_ids(self):
        return self._csv_to_list(self.database_subnet_ids)

    @property
    def security_group_ids(self):
        return self._csv_to_list(self.database_security_group_ids)

    @property
    def transition_map(self):
        return {
            "wait": ["restore", "alarm"],
            "restore": ["modify", "alarm"],
            "modify": ["verify", "alarm"],
            "verify": ["cleanup", "alarm"],
            "cleanup": ["wait", "alarm"],
            "alarm": ["cleanup", "alarm"],
        }


def create_dbsnap_verify_state_doc(
    database,
    database_subnet_ids,
    database_security_group_ids,
    snapshot_region,
    **kwargs
):
    state_doc = DbsnapVerifyStateDoc(
        database,
        database_subnet_ids=database_subnet_ids,
        database_security_group_ids=database_security_group_ids,
        snapshot_region=snapshot_region,
        **kwargs
    )
    state_doc.transition_state("wait", validate=False)
    return state_doc


def get_state_doc_from_sns_event(event):
    """Return state_doc (or None) for a RDS event, instead of config event."""
    try:
        event_payload = json.loads(event["Records"][0]["Sns"]["Message"])
        # strip "dbsnap-verify-" from tmp_database name.
        database_id = event_payload["Source ID"][DB_ID_PREFIX_LEN:]
        rds_event_message = event_payload["Event Message"]

    except KeyError:
        return None

    state_doc = DbsnapVerifyStateDoc(
        database_id, rds_event_latest_message=rds_event_message
    )

    return state_doc


def is_config_event(event):
    if "database" in event:
        return True
    return False


def get_or_create_state_doc(event):
    if is_config_event(event):
        # Try to load StateDoc from config event.
        state_doc = DbsnapVerifyStateDoc(**event)
    else:
        # Try to load StateDoc from sns rds event.
        state_doc = get_state_doc_from_sns_event(event)

    if state_doc:
        try:
            # try to load the state_doc.
            state_doc.load()
        except (boto3.client("s3").exceptions.NoSuchKey, IOError):
            if is_config_event(event):
                # create the state_doc if it doesn't exist.
                state_doc = create_dbsnap_verify_state_doc(**event)
            else:
                state_doc = None

    return state_doc
