import numpy as np
import redis
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import os
from urllib.parse import urlparse

erddap_url = 'http://ecofoci-field.pmel.noaa.gov:8080/erddap'
erddap_datasetID = ['2024_Argos_Drifters_NRT','2024_Argos_Drifters_NRT']

meta_variables = ['longitude', 'latitude', 'time',]
surface_variables = ['sst','strain','location_quality',]
unused_variables = []
depth_variables = []

units = {
    'location_quality':'',
    'sst': 'Deg C',
    'strain': '%',
}

all_variables_comma_separated = ','.join(surface_variables) + ',' + ','.join(meta_variables)

dtypes = {
    'row': int,
    'trajectory_id': str,
    'latitude': np.float64,
    'longitude': np.float64,
    'location_quality': np.float64,
    'sst': np.float64,
    'strain': np.float64,
}

platforms = [
]

# Define a redis instance. This definition will work both locally and with an app deployed to DE:
if os.environ.get("DASH_ENTERPRISE_ENV") == "WORKSPACE":
    parsed_url = urlparse(os.environ.get("REDIS_URL"))
    if parsed_url.path == "" or parsed_url.path == "/":
        i = 0
    else:
        try:
            i = int(parsed_url.path[1:])
        except:
            raise Exception("Redis database should be a number")
    parsed_url = parsed_url._replace(path="/{}".format((i + 1) % 16))

    updated_url = parsed_url.geturl()
    REDIS_URL = "redis://%s" % (updated_url.split("://")[1])
else:
    REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

redis_instance = redis.StrictRedis.from_url(
    os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
)

data_table = 'argo'
counts_table = 'counts'
locations_table = 'locations'

# Create a SQLAlchemy connection string from the environment variable `DATABASE_URL`
# automatically created in your dash app when it is linked to a postgres container
# on Dash Enterprise. If you're running locally and `DATABASE_URL` is not defined,
# then this will fall back to a connection string for a local postgres instance
#  with username='postgres' and password='password'
connection_string = "postgresql+pg8000" + os.environ.get(
    "DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5432"
).lstrip("postgresql")

# Create a SQLAlchemy engine object. This object initiates a connection pool
# so we create it once here and import into app.py.
# `poolclass=NullPool` prevents the Engine from using any connection more than once. You'll find more info here:
# https://docs.sqlalchemy.org/en/14/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork
postgres_engine = create_engine(connection_string, poolclass=NullPool)