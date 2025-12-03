from dash import Dash, html, dcc, callback, Input, Output
import dash_ag_grid as dag
import dash


import pandas as pd
import psycopg2
import dotenv
dash.register_page(__name__)

conn_string = dotenv.get_key(".env", "CONN_STRING")
conn = psycopg2.connect(conn_string)

df = pd.read_sql_query("SELECT * from papers LIMIT 50", conn)

layout = html.Div([
    html.Div(children='My First App with Data'),
    dag.AgGrid(
        rowData=df.to_dict('records'),
        columnDefs=[{"field": i} for i in df.columns]
    ),
])


