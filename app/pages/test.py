from dash import Dash, html, dcc, callback, Input, Output
import dash_ag_grid as dag
import dash


import pandas as pd
from database import engine 
dash.register_page(__name__)


with engine.connect() as connection:
    df = pd.read_sql_query("SELECT * from papers LIMIT 50", connection)

layout = html.Div([
    html.Div(children='My First App with Data'),
    dag.AgGrid(
        rowData=df.to_dict('records'),
        columnDefs=[{"field": i} for i in df.columns]
    ),
])


