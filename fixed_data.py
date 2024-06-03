import pandas as pd

def get_fixed_ts(erddap_url,datasetID):
    df = pd.read_csv(f'{erddap_url}/tabledap/{datasetID}.csv?',skiprows=[1], parse_dates=True)
    return df