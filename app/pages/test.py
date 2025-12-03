from dash import Dash, html, dcc, callback, Input, Output
import dash_ag_grid as dag
import dash


import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import dotenv
dash.register_page(__name__)

conn_string = dotenv.get_key(".env", "CONN_STRING")

engine = create_engine(conn_string, echo=True)

with engine.connect() as connection:
    df = pd.read_sql_query("SELECT * from papers LIMIT 50", connection)

layout = html.Div([
    html.Div(children='My First App with Data'),
    dag.AgGrid(
        rowData=df.to_dict('records'),
        columnDefs=[{"field": i} for i in df.columns]
    ),
])


