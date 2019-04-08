def describe_db_instances(session, identifier):
    """Returns the description of a RDS db instance.
    Args:
        identifier (str): an RDS resource identifier.
    Returns:
        List: A list with the RDS instance informations.
    """
    return session.describe_db_instances(DBInstanceIdentifier=identifier)
