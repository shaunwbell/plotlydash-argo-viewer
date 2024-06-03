import os
import json
import redis
import pandas as pd

from dash import Dash, html, dcc
import dash_design_kit as ddk
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import db
import fixed_data as fxd
import constants

import tasks
from theme import theme

###

app = Dash(__name__)
server = app.server  # expose server variable for Procfile

def serve_layout():    

    def map_figures(df):
        fig = px.scatter_geo(df, lat='latitude', lon='longitude', hover_data=['text_time'],
                            hover_name="trajectory_id", projection='orthographic', color='trajectory_id'
                            )
        fig.update_geos(resolution=50,lataxis_showgrid=True, lonaxis_showgrid=True,
                        projection_rotation=dict(lon=200, lat=50, roll=0),
                        lataxis_range=[52,72.5], lonaxis_range=[180,240])
        fig.update_layout(height=300, margin={"r":0,"t":0,"l":0,"b":0})
        
        return fig

    def empty_figures():
        fig = px.scatter_geo(projection='orthographic')
        fig.update_geos(resolution=50,lataxis_showgrid=True, lonaxis_showgrid=True,
                        projection_rotation=dict(lon=200, lat=50, roll=0),
                        lataxis_range=[52,72.5], lonaxis_range=[180,240])
        fig.update_layout(height=300, margin={"r":0,"t":0,"l":0,"b":0})

        return fig

    def time_figures(df,id_var='trajectory_id'):
        fig = make_subplots(rows=3, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05)
        colorwheel=["black","blue","red","black"] * 5 #total number of units in database
        for group,gdata in df.groupby(id_var):
            color=colorwheel.pop()
            fig.add_trace(go.Scatter(x=gdata['time'], y=gdata['sst'],
                        mode='markers', name=group, marker={"color":color},legendgroup=group
                        ),row=1,col=1)    
            fig.add_trace(go.Scatter(x=gdata['time'], y=gdata['strain'],
                        mode='markers', name=group, marker={"color":color},legendgroup=group,showlegend=False
                        ),row=2,col=1)      
            fig.add_trace(go.Scatter(x=gdata['time'], y=gdata['location_quality'],
                        mode='markers', name=group, marker={"color":color},legendgroup=group,showlegend=False
                        ),row=3,col=1)  
        return fig

    try:
        new_df = db.get_data()
        locmap = map_figures(new_df)
    except:
        locmap = empty_figures()

    #sst
    try:
        timefig_T1 = time_figures(new_df)
    except:
        timefig_T1 = time_figures(new_df)

    ##bottom
    try:
        new_bdf = fxd.get_fixed_ts(constants.erddap_url,constants.erddap_datasetID[1])
    except:
        new_bdf = pd.DataFrame()

    try:
        timefig_bT1 = time_figures(new_bdf,id_var='timeseries_id')
    except:
        timefig_bT1 = None


    return ddk.App(
        [
            ddk.Header(
                [
                    ddk.Logo(src=app.get_asset_url("logo.png")),
                    ddk.Title("Interactive EcoFOCI Active ARGO Drifter Dashboard (UNQC'd) "),
                ]
            ),
            ddk.Card(
                width=50,
                children=[
                ddk.CardHeader(title="Unit Location Analysis"),
                ddk.Graph(
                    style={'height': '700px'},
                    id="graph-map",
                    figure=locmap,
            )]),
            ddk.Card(
                dcc.Tabs([
                dcc.Tab(label='SST', children=[
                    ddk.CardHeader(title="SST Timeseries Analysis"),
                    ddk.Graph(
                        style={'height': '600px'},
                        id="graph-timeseries_T1",
                        figure=timefig_T1,
                )]),
                dcc.Tab(label='Bottom', children=[
                    ddk.CardHeader(title="Drifter Timeseries Strain Analysis"),
                    ddk.Graph(
                        style={'height': '600px'},
                        id="graph-timeseries_T1B",
                        figure=timefig_bT1,
                )])]                ),
                width=50,                
            ),
        ],
        theme=theme,
    )


# Set layout to a function so that it is called on-the-fly when the
# application is loaded in the web browser
app.layout = serve_layout

if __name__ == "__main__":
    app.run_server()
