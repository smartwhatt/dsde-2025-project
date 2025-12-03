from dash import Dash, html, dcc, callback, Input, Output
import dash_ag_grid as dag
import dash


import pandas as pd
import psycopg2
import dotenv

import plotly.graph_objects as go #edit
dash.register_page(__name__)

conn_string = dotenv.get_key(".env", "CONN_STRING")
conn = psycopg2.connect(conn_string)

#edit '''
top_authors_query = """
    SELECT 
        a.indexed_name,
        COUNT(DISTINCT pa.paper_id) AS paper_count
    FROM authors a
    JOIN paper_authors pa ON a.author_id = pa.author_id
    GROUP BY a.author_id, a.indexed_name
    ORDER BY paper_count DESC
    LIMIT 10;
"""

papers_by_year_query = """
    SELECT 
        publication_year,
        COUNT(*) AS paper_count
    FROM papers
    WHERE publication_year BETWEEN 2018 AND 2023
    GROUP BY publication_year
    ORDER BY publication_year;
"""

# --- Load data into DataFrames ---
df_authors = pd.read_sql_query(top_authors_query, conn)
df_years = pd.read_sql_query(papers_by_year_query, conn)

# --- Build figures ---

# Bar chart – top authors
fig_authors = go.Figure([
    go.Bar(
        x=df_authors["indexed_name"],
        y=df_authors["paper_count"],
        text=df_authors["paper_count"],
        textposition="outside",
        marker_color="#1f77b4",
    )
])
fig_authors.update_layout(
    title="Top 10 Authors by Number of Papers",
    xaxis_title="Author Name",
    yaxis_title="Number of Papers",
    template="plotly_white",
    xaxis={"tickangle": -45},
    height=500,
)

# Line chart – papers per year
fig_years = go.Figure([
    go.Scatter(
        x=df_years["publication_year"],
        y=df_years["paper_count"],
        mode="lines+markers",
        marker=dict(size=10, color="#2ca02c"),
        line=dict(width=3, color="#2ca02c"),
    )
])
fig_years.update_layout(
    title="Number of Papers Published (2018–2023)",
    xaxis_title="Year",
    yaxis_title="Number of Papers",
    template="plotly_white",
    height=500,
    xaxis=dict(
        tickmode="linear",
        dtick=1,
    ),
)
#edit '''
df = pd.read_sql_query("SELECT * from papers LIMIT 50", conn)

layout = html.Div([
    html.H1(
    "Papers Dashboard",
    style={'textAlign': 'center', 'marginBottom': 30}
),

# Top Authors Section
html.Div([
    html.H2("Top 10 Authors by Paper Count", style={'marginBottom': 20}),
    dcc.Graph(
        id="top-authors-chart",
        figure=fig_authors
    )
], style={'marginBottom': 50}),

# Papers by Year Section
html.Div([
    html.H2("Publications Timeline (2018–2023)", style={'marginBottom': 20}),
    dcc.Graph(
        id="papers-by-year-chart",
        figure=fig_years
    )
], style={'marginBottom': 50}),

# Summary Statistics
html.Div([
    html.H3("Summary Statistics", style={'marginBottom': 20}),
    html.Div([
        # Total papers
        html.Div([
            html.H4("Total Papers (2018–2023)", style={'color': '#666'}),
            html.H2(f"{df_years['paper_count'].sum():,}",
                    style={'color': '#1f77b4'})
        ], style={
            'padding': 20,
            'backgroundColor': '#f8f9fa',
            'borderRadius': 10,
            'marginRight': 20,
            'flex': 1
        }),

        # Average per year
        html.Div([
            html.H4("Average Papers per Year", style={'color': '#666'}),
            html.H2(f"{df_years['paper_count'].mean():.0f}",
                    style={'color': '#2ca02c'})
        ], style={
            'padding': 20,
            'backgroundColor': '#f8f9fa',
            'borderRadius': 10,
            'marginRight': 20,
            'flex': 1
        }),

        # Most productive author
        html.Div([
            html.H4("Most Productive Author", style={'color': '#666'}),
            html.H2(f"{df_authors.iloc[0]['paper_count']} papers",
                    style={'color': '#ff7f0e', 'fontSize': 24})
        ], style={
            'padding': 20,
            'backgroundColor': '#f8f9fa',
            'borderRadius': 10,
            'flex': 1
        })
    ], style={'display': 'flex', 'justifyContent': 'space-between'})
], style={'marginBottom': 50})
])


