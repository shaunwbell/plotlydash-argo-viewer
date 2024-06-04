import constants
import pandas as pd
import datetime

from sqlalchemy import MetaData, Table

def trim(days_ago):
    n = datetime.datetime.now()
    r = n - datetime.timedelta(days=days_ago)
    delete = 'DELETE FROM {} WHERE TIME < \'' + r.isoformat() + '\';'
    print(delete)
    constants.postgres_engine.execute(delete.format(constants.data_table))


def get_between_days_ago(ago1, ago2):
    if ago1 == ago2:
        return

    if ago1 > ago2:
        t = ago1
        ago1 = ago2
        ago2 = t

    since1 = datetime.datetime.now() - datetime.timedelta(days = ago1) 
    since2 = datetime.datetime.now() - datetime.timedelta(days = ago2) 
    selection = 'SELECT * from {} WHERE TIME BETWEEN \''+since2.isoformat()+'\' AND \''+since1.isoformat()+'\' ORDER BY TIME';
    stored_df = pd.read_sql(
        selection.format(constants.data_table), constants.postgres_engine
    )
    return stored_df


def drop_all():
    metadata = MetaData(bind=constants.postgres_engine, reflect=True)
    d_table = Table(constants.data_table, metadata)
    d_table.drop(constants.postgres_engine, checkfirst=True)
    c_table = Table(constants.counts_table, metadata)
    c_table.drop(constants.postgres_engine, checkfirst=True)
    l_table = Table(constants.locations_table, metadata)
    l_table.drop(constants.postgres_engine, checkfirst=True)

def get_data():
    # In this function, we retrieve the data from postgres using pandas's read_sql method.

    # This data is periodically getting updated via a separate Celery Process in tasks.py.
    # "dataset_table" is the name of the table that we initialized in tasks.py.
    selection = 'SELECT * from {}'
    selection = selection + ' ORDER BY TIME;'
    updated_df = pd.read_sql(
        selection.format(constants.data_table), constants.postgres_engine
    )
    return updated_df

def test_selection():
    selection = 'SELECT * from {}'
    selection = selection + ' ORDER BY TIME LIMIT 10;'
    updated_df = pd.read_sql(
        selection.format(constants.data_table), constants.postgres_engine
    )
    return updated_df

def exists():
    try:
        df = test_selection()
    except:
        return False

    if df is not None:
        return True
    else:
        return False


def get_counts():
    # In this function, we retrieve the data from postgres using pandas's read_sql method.

    # This data is periodically getting updated via a separate Celery Process in tasks.py.
    # "dataset_table" is the name of the table that we initialized in tasks.py.
    updated_df = pd.read_sql(
        "SELECT * FROM {};".format(constants.counts_table), constants.postgres_engine
    )
    return updated_df


def get_locations():
    # In this function, we retrieve the data from postgres using pandas's read_sql method.

    # This data is periodically getting updated via a separate Celery Process in tasks.py.
    # "dataset_table" is the name of the table that we initialized in tasks.py.
    updated_df = pd.read_sql(
        "SELECT * FROM {};".format(constants.locations_table), constants.postgres_engine
    )
    return updated_df

df = get_data()
print(df.columns)