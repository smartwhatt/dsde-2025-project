from dash import Dash, html, dcc
import dash_ag_grid as dag
import dash


import pandas as pd
import psycopg2
import dotenv

app = Dash(__name__, use_pages=True)

# Requires Dash 2.17.0 or later
app.layout = [
    
]



if __name__ == '__main__':
    app.run(debug=True)