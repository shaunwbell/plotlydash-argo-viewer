from tasks import load_observations

# Call the Celery task that pre-populates the Postgres DB with the updated dataframe
# when the app is deployed (i.e. when this file is executed)
load_observations()