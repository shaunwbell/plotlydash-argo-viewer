from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import os
import pandas as pd

import constants

# Functions used from the command line to set up and/or maintain the database.


def delete_all_files():
    delete = "DELETE FROM {};"
    constants.postgres_engine.execute(delete.format(constants.data_table))


def drop_all():
    drop_files()
    drop_submissions()
    constants.postgres_engine.execute("DROP TYPE file_type;")
    create_submissions()
    create_files()
    constants.postgres_engine.execute(
        "DROP FUNCTION IF EXISTS sync_modified_submissions();"
    )
    constants.postgres_engine.execute("DROP FUNCTION IF EXISTS sync_modified_files();")
    modfied_submissions = f"""
        CREATE FUNCTION sync_modified_submissions() RETURNS trigger AS $$
            BEGIN
            NEW.modified:= NOW();

        RETURN NEW;
            END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER
            sync_modifed_submissions
        BEFORE UPDATE ON
            {constants.submissions_table}
        FOR EACH ROW EXECUTE PROCEDURE
            sync_modified_submissions();
    """
    modfied_files = f"""
        CREATE FUNCTION sync_modified_files() RETURNS trigger AS $$
            BEGIN
            NEW.modified:= NOW();

        RETURN NEW;
            END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER
            sync_modifed_files
        BEFORE UPDATE ON
            {constants.data_table}
        FOR EACH ROW EXECUTE PROCEDURE
            sync_modified_files();
    """
    constants.postgres_engine.execute(modfied_submissions)
    constants.postgres_engine.execute(modfied_files)


def drop_files():
    delete = "DROP TABLE {};"
    constants.postgres_engine.execute(delete.format(constants.data_table))


def delete_all_submissions():
    delete = "DELETE FROM {};"
    constants.postgres_engine.execute(delete.format(constants.submissions_table))


def drop_submissions():
    delete = "DROP TABLE {};"
    constants.postgres_engine.execute(delete.format(constants.submissions_table))


def create_submissions():
    create = """
        CREATE TABLE {} (
            id SERIAL,
            submission VARCHAR PRIMARY KEY NOT NULL UNIQUE,
            user_id VARCHAR NOT NULL,
            modified TIMESTAMP NOT NULL DEFAULT now(),
            created TIMESTAMP NOT NULL DEFAULT now()
        );
    """
    constants.postgres_engine.execute(create.format(constants.submissions_table))


def create_files():
    enum = "CREATE TYPE file_type AS ENUM ('data', 'metadata', 'unknown');"
    constants.postgres_engine.execute(enum)
    create = """
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            submission VARCHAR references {}(submission) NOT NULL,
            user_id VARCHAR NOT NULL,
            filename VARCHAR NOT NULL,
            category file_type NOT NULL,
            mime VARCHAR NOT NULL,
            modified TIMESTAMP NOT NULL DEFAULT now(),
            created TIMESTAMP NOT NULL DEFAULT now()
        );
    """
    constants.postgres_engine.execute(
        create.format(constants.data_table, constants.submissions_table)
    )