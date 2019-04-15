from dbsnap.rds_funcs import (
    get_latest_snapshot,
    restore_from_latest_snapshot,
    create_cluster_instance,
    modify_instance_or_cluster_for_verify,
    delete_verified_database,
    destroy_database_subnet_group,
)

from dbsnap.database import Database

from .state_doc import get_or_create_state_doc

from .datadog_output import datadog_lambda_check_output

from .utils import describe_db_instances

import boto3
import psycopg2
import re

# retry 3 times on errors.
from botocore.config import Config

BOTO3_CONFIG = Config(retries={"max_attempts": 3})

import logging

logger = logging.getLogger("dbsnap")


def dbsnap_verify_datadog_output(state_doc, alarm_status="OK"):
    return datadog_lambda_check_output(
        metric_name="dbsnap-verify.status",
        metric_value=alarm_status,
        metric_tags={"database": state_doc.database},
    )


def wait(state_doc, rds_session):
    """wait: currently waiting for the next snapshot to appear."""
    logger.info(
        "Looking for a snapshot of %s (newer than %s)",
        state_doc.database,
        state_doc.snapshot_verified,
    )
    snapshot = get_latest_snapshot(rds_session, state_doc.database)
    if snapshot.id != state_doc.snapshot_verified:
        # if the latest snapshot is not equal to the most recently
        # verified snapshot, restore and verify it.
        state_doc.snapshot_verifying = snapshot.id
        state_doc.transition_state("restore")
        restore(state_doc, rds_session)
    else:
        logger.info(
            "Did not find a snapshot of %s (newer than %s)",
            state_doc.database,
            state_doc.snapshot_verified,
        )
        logger.info("Going to sleep.")


def restore(state_doc, rds_session):
    """restore: currently restoring a copy of the latest
    snapshot into a temporary RDS db instance."""
    tmp_database = Database(session=rds_session, identifier=state_doc.tmp_database)
    if not tmp_database:
        logger.info(
            "Restoring snapshot of %s to %s", state_doc.database, state_doc.tmp_database
        )
        restore_from_latest_snapshot(
            rds_session, state_doc.database, state_doc.subnet_ids
        )
    elif tmp_database.status == "available":
        if tmp_database.is_cluster:
            if not tmp_database.cluster_member_ids:
                logger.info(
                    "Creating cluster member instance for cluster (%s)", tmp_database.id
                )
                instance_identifier = "i-{}".format(tmp_database.id)
                create_cluster_instance(tmp_database, instance_identifier)
                # exit early, we need an available cluster instance to continue.
                return None

            cluster_member = tmp_database.cluster_members[0]

            if cluster_member.status != "available":
                logger.info(
                    "Waiting for cluster member instance to become available (%s)",
                    cluster_member.id,
                )
                # exit early, we need an available cluster instance to continue.
                return None

        state_doc.transition_state("modify")
        modify(state_doc, rds_session)

    else:
        logger.info(
            "Still restoring snapshot of %s to %s",
            state_doc.database,
            state_doc.tmp_database,
        )


def modify(state_doc, rds_session):
    """modify: currently modifying the temporary RDS db instance
    settings to allow the dbsnap-verify tool to access it."""
    tmp_database = Database(session=rds_session, identifier=state_doc.tmp_database)
    if state_doc.tmp_password is None:
        logger.info(
            "Modifying %s master password and security groups", state_doc.tmp_database
        )
        state_doc.tmp_password = modify_instance_or_cluster_for_verify(
            tmp_database, state_doc.security_group_ids
        )
        state_doc.save()
    elif (
        tmp_database
        and tmp_database.status == "available"
        and "Reset master credentials" in tmp_database.event_messages
    ):
        state_doc.transition_state("verify")
        verify(state_doc, rds_session)
    else:
        logger.info(
            "Waiting for master credentials reset for %s", state_doc.tmp_database
        )

def verify(state_doc, rds_session):
    """verify: currently verifying the temporary RDS db instance
    using the supplied checks."""
    instances = describe_db_instances(rds_session, state_doc.tmp_database)
    rds_engine = instances.get('DBInstances')[0].get('Engine')
    rds_address = instances.get('DBInstances')[0].get('Endpoint').get('Address')
    rds_port = instances.get('DBInstances')[0].get('Endpoint').get('Port')
    rds_masterUsername = instances.get('DBInstances')[0].get('MasterUsername')
    rds_DBName = instances.get('DBInstances')[0].get('DBName')
    if rds_engine == "postgres":
        conn = psycopg2.connect(host=rds_address, port=rds_port, user=rds_masterUsername, password=state_doc.tmp_password, database=rds_DBName)
    logger.info("Running checks on %s", state_doc.tmp_database)
    success = True          
    for check in state_doc.checks:
        if check.get("type") == "query":
            cursor = conn.cursor()
            cursor.execute(check.get("query"))
            r = cursor.fetchall()
            results = [item for item, in r]
            r = re.compile(check.get("regex"))
            match = list(filter(r.match, results))
            try:
                if len(match) > 0:
                    logger.info("The result is %s", match)
                    logger.info(
                        "Found regex result in %s", state_doc.tmp_database
                    )
            except Exception as e:
                print(e)
                logger.info(
                    "Didnt find regex result in %s", state_doc.tmp_database
                )
                logger.info(dbsnap_verify_datadog_output(state_doc, "CRITICAL"))
                success = False
                break
    if not state_doc.checks:
        logger.info(
            "No checks to run on %s", state_doc.tmp_database
            )
    elif success:
        logger.info(dbsnap_verify_datadog_output(state_doc, "OK"))
    state_doc.transition_state("cleanup")
    cleanup(state_doc, rds_session)


def cleanup(state_doc, rds_session):
    """clean: currently tearing down the temporary RDS db instance
    and anything else we created or modified."""
    tmp_database = Database(session=rds_session, identifier=state_doc.tmp_database)
    if not tmp_database:
        # cleanup of db subnet group, tmp_password, and transition to wait.
        logger.info("cleaning %s subnet group and tmp_password", state_doc.tmp_database)
        destroy_database_subnet_group(rds_session, state_doc.tmp_database)
        # remove tmp_password, clear old states.
        state_doc.clean()
        # wait for next snapshot (which could appear tomorrow).
        state_doc.transition_state("wait")
    elif tmp_database.status == "available":
        logger.info("cleaning / destroying %s", state_doc.tmp_database)
        delete_verified_database(tmp_database)
    else:
        logger.info("still cleaning / destroying %s", state_doc.tmp_database)


def alarm(state_doc, rds_session):
    """"alarm: something went wrong we are going to scream about it."""
    logger.error(dbsnap_verify_datadog_output(state_doc, "CRITICAL"))


state_handlers = {
    "wait": wait,
    "restore": restore,
    "modify": modify,
    "verify": verify,
    "cleanup": cleanup,
    "alarm": alarm,
}


def handler(event):
    """The main entrypoint called from CLI or when our AWS Lambda wakes up."""
    logger.debug("%s", event)
    state_doc = get_or_create_state_doc(event)
    if state_doc is None:
        # A state_doc is None if we receive an invalid or unrelated event
        # from from Cloudwatch or SNS, like an unrelated RDS db instance.
        logger.info("Ignoring unrelated RDS event.")
    else:
        state_handler = state_handlers[state_doc.current_state]
        rds_session = boto3.client(
            "rds", region_name=state_doc.snapshot_region, config=BOTO3_CONFIG
        )
        state_handler(state_doc, rds_session)
