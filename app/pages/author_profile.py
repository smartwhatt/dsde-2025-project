import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd
from sqlalchemy import text
import dotenv
import plotly.express as px
import plotly.graph_objects as go

from database import engine 


dash.register_page(__name__, path_template="/author/<author_id>")



def get_author_basic_info(author_id):
    """Fetch basic author information."""
    try:
        query = text("""
            SELECT 
                a.author_id,
                a.indexed_name,
                a.auid,
                a.surname,
                a.given_name,
                COUNT(DISTINCT pa.paper_id) AS paper_count,
                COALESCE(SUM(p.cited_by_count), 0) AS total_citations,
                MIN(p.publication_year) AS first_year,
                MAX(p.publication_year) AS last_year
            FROM authors a
            LEFT JOIN paper_authors pa ON a.author_id = pa.author_id
            LEFT JOIN papers p ON pa.paper_id = p.paper_id
            WHERE a.author_id = :author_id
            GROUP BY a.author_id, a.indexed_name, a.auid, a.surname, a.given_name
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        return df.iloc[0] if not df.empty else None
    except Exception as e:
        print(f"Error fetching author info: {e}")
        return None


def get_papers_by_year(author_id):
    """Fetch publication and citation trends by year."""
    try:
        query = text("""
            SELECT 
                p.publication_year,
                COUNT(DISTINCT p.paper_id) AS paper_count,
                COALESCE(SUM(p.cited_by_count), 0) AS total_citations
            FROM papers p
            JOIN paper_authors pa ON p.paper_id = pa.paper_id
            WHERE pa.author_id = :author_id
                AND p.publication_year IS NOT NULL
            GROUP BY p.publication_year
            ORDER BY p.publication_year
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        return df
    except Exception as e:
        print(f"Error fetching papers by year: {e}")
        return pd.DataFrame()


def get_top_cited_papers(author_id):
    """Fetch top 10 most cited papers."""
    try:
        query = text("""
            SELECT 
                p.paper_id,
                p.title,
                p.publication_year,
                p.cited_by_count,
                s.source_name,
                STRING_AGG(DISTINCT a.indexed_name, ', ' ORDER BY a.indexed_name) as all_authors
            FROM papers p
            JOIN paper_authors pa ON p.paper_id = pa.paper_id
            LEFT JOIN sources s ON p.source_id = s.source_id
            LEFT JOIN paper_authors pa2 ON p.paper_id = pa2.paper_id
            LEFT JOIN authors a ON pa2.author_id = a.author_id
            WHERE pa.author_id = :author_id
            GROUP BY p.paper_id, p.title, p.publication_year, p.cited_by_count, s.source_name
            ORDER BY p.cited_by_count DESC
            LIMIT 10
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        if not df.empty:
            df["title"] = df["title"].apply(
                lambda x: x[:100] + "..." if x and len(str(x)) > 100 else x
            )
        return df
    except Exception as e:
        print(f"Error fetching top cited papers: {e}")
        return pd.DataFrame()


def get_top_collaborators(author_id):
    """Fetch top 10 collaborators."""
    try:
        query = text("""
            SELECT 
                a.author_id,
                a.indexed_name,
                COUNT(DISTINCT pa1.paper_id) as collaboration_count
            FROM paper_authors pa1
            JOIN paper_authors pa2 ON pa1.paper_id = pa2.paper_id
            JOIN authors a ON pa2.author_id = a.author_id
            WHERE pa1.author_id = :author_id
                AND pa2.author_id != :author_id
            GROUP BY a.author_id, a.indexed_name
            ORDER BY collaboration_count DESC
            LIMIT 10
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        return df
    except Exception as e:
        print(f"Error fetching collaborators: {e}")
        return pd.DataFrame()


def get_subject_areas(author_id):
    """Fetch top subject areas."""
    try:
        query = text("""
            SELECT 
                sa.subject_name,
                COUNT(DISTINCT psa.paper_id) as paper_count
            FROM subject_areas sa
            JOIN paper_subject_areas psa ON sa.subject_area_id = psa.subject_area_id
            JOIN paper_authors pa ON psa.paper_id = pa.paper_id
            WHERE pa.author_id = :author_id
            GROUP BY sa.subject_area_id, sa.subject_name
            ORDER BY paper_count DESC
            LIMIT 10
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        return df
    except Exception as e:
        print(f"Error fetching subject areas: {e}")
        return pd.DataFrame()


def get_top_keywords(author_id):
    """Fetch top keywords."""
    try:
        query = text("""
            SELECT 
                k.keyword,
                COUNT(DISTINCT pk.paper_id) as paper_count
            FROM keywords k
            JOIN paper_keywords pk ON k.keyword_id = pk.keyword_id
            JOIN paper_authors pa ON pk.paper_id = pa.paper_id
            WHERE pa.author_id = :author_id
            GROUP BY k.keyword_id, k.keyword
            ORDER BY paper_count DESC
            LIMIT 15
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        return df
    except Exception as e:
        print(f"Error fetching keywords: {e}")
        return pd.DataFrame()


def get_top_sources(author_id):
    """Fetch top publication venues."""
    try:
        query = text("""
            SELECT 
                s.source_name,
                COUNT(DISTINCT p.paper_id) as paper_count
            FROM sources s
            JOIN papers p ON s.source_id = p.source_id
            JOIN paper_authors pa ON p.paper_id = pa.paper_id
            WHERE pa.author_id = :author_id
            GROUP BY s.source_id, s.source_name
            ORDER BY paper_count DESC
            LIMIT 10
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        if not df.empty:
            df["source_name"] = df["source_name"].apply(
                lambda x: x[:60] + "..." if x and len(str(x)) > 60 else x
            )
        return df
    except Exception as e:
        print(f"Error fetching sources: {e}")
        return pd.DataFrame()


def get_affiliations(author_id):
    """Fetch all affiliations."""
    try:
        query = text("""
            SELECT DISTINCT
                af.affiliation_name,
                af.country,
                COUNT(DISTINCT pa.paper_id) as paper_count
            FROM affiliations af
            JOIN paper_author_affiliations paa ON af.affiliation_id = paa.affiliation_id
            JOIN paper_authors pa ON paa.paper_author_id = pa.paper_author_id
            WHERE pa.author_id = :author_id
            GROUP BY af.affiliation_id, af.affiliation_name, af.country
            ORDER BY paper_count DESC
        """)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        return df
    except Exception as e:
        print(f"Error fetching affiliations: {e}")
        return pd.DataFrame()


def layout(author_id=None, **kwargs):
    if not author_id:
        return dbc.Container([
            html.H3("Author not found", className="text-center mt-5 text-danger")
        ])
    
    try:
        author_id = int(author_id)
    except:
        return dbc.Container([
            html.H3("Invalid author ID", className="text-center mt-5 text-danger")
        ])
    
    author_info = get_author_basic_info(author_id)
    
    if author_info is None:
        return dbc.Container([
            html.H3("Author not found", className="text-center mt-5 text-danger")
        ])
    
    df_by_year = get_papers_by_year(author_id)
    df_top_cited = get_top_cited_papers(author_id)
    df_collaborators = get_top_collaborators(author_id)
    df_subjects = get_subject_areas(author_id)
    df_keywords = get_top_keywords(author_id)
    df_sources = get_top_sources(author_id)
    df_affiliations = get_affiliations(author_id)
    
    # Create visualizations
    fig_publications = go.Figure()
    if not df_by_year.empty:
        fig_publications.add_trace(go.Scatter(
            x=df_by_year['publication_year'],
            y=df_by_year['paper_count'],
            mode='lines+markers',
            name='Publications',
            line=dict(color='#0d6efd', width=3),
            marker=dict(size=8)
        ))
        fig_publications.update_layout(
            title="Publications Over Time",
            xaxis_title="Year",
            yaxis_title="Number of Papers",
            template="plotly_white",
            height=350,
            hovermode='x unified'
        )
    
    fig_citations = go.Figure()
    if not df_by_year.empty:
        fig_citations.add_trace(go.Bar(
            x=df_by_year['publication_year'],
            y=df_by_year['total_citations'],
            name='Citations',
            marker_color='#198754'
        ))
        fig_citations.update_layout(
            title="Citations by Publication Year",
            xaxis_title="Year",
            yaxis_title="Citations",
            template="plotly_white",
            height=350
        )
    
    fig_subjects = go.Figure()
    if not df_subjects.empty:
        fig_subjects = px.pie(
            df_subjects,
            values='paper_count',
            names='subject_name',
            title="Research Areas Distribution"
        )
        fig_subjects.update_layout(
            template="plotly_white",
            height=400
        )
    
    fig_keywords = go.Figure()
    if not df_keywords.empty:
        fig_keywords = px.bar(
            df_keywords,
            x='paper_count',
            y='keyword',
            orientation='h',
            title="Top 15 Keywords"
        )
        fig_keywords.update_layout(
            template="plotly_white",
            height=500,
            yaxis={'categoryorder': 'total ascending'}
        )
    
    avg_citations = author_info['total_citations'] / author_info['paper_count'] if author_info['paper_count'] > 0 else 0
    years_active = author_info['last_year'] - author_info['first_year'] + 1 if author_info['last_year'] and author_info['first_year'] else 0
    avg_papers_per_year = author_info['paper_count'] / years_active if years_active > 0 else 0
    
    return dbc.Container([
        # Header
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H1(author_info['indexed_name'], className="display-4 fw-bold text-dark mb-2"),
                    html.P([
                        html.I(className="bi bi-person-badge me-2"),
                        f"Scopus ID: {author_info['auid']}"
                    ], className="text-muted fs-5 mb-1"),
                    html.P([
                        html.I(className="bi bi-calendar-range me-2"),
                        f"Active: {author_info['first_year']} - {author_info['last_year']}"
                    ], className="text-muted fs-5") if author_info['first_year'] and author_info['last_year'] else None,
                ], className="text-center"),
                html.Hr(),
            ], width=12)
        ], className="mt-4 mb-3"),
        
        # Key Metrics Cards
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.I(className="bi bi-file-text fs-1 text-primary mb-2"),
                        html.H3(f"{author_info['paper_count']:,}", className="mb-1 text-primary"),
                        html.P("Total Papers", className="text-muted mb-0 fw-semibold")
                    ], className="text-center")
                ], className="shadow-sm h-100")
            ], md=3, sm=6, className="mb-3"),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.I(className="bi bi-quote fs-1 text-success mb-2"),
                        html.H3(f"{author_info['total_citations']:,}", className="mb-1 text-success"),
                        html.P("Total Citations", className="text-muted mb-0 fw-semibold")
                    ], className="text-center")
                ], className="shadow-sm h-100")
            ], md=3, sm=6, className="mb-3"),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.I(className="bi bi-bar-chart fs-1 text-warning mb-2"),
                        html.H3(f"{avg_citations:.1f}", className="mb-1 text-warning"),
                        html.P("Avg Citations/Paper", className="text-muted mb-0 fw-semibold")
                    ], className="text-center")
                ], className="shadow-sm h-100")
            ], md=3, sm=6, className="mb-3"),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.I(className="bi bi-calendar-check fs-1 text-info mb-2"),
                        html.H3(f"{avg_papers_per_year:.1f}", className="mb-1 text-info"),
                        html.P("Papers/Year", className="text-muted mb-0 fw-semibold")
                    ], className="text-center")
                ], className="shadow-sm h-100")
            ], md=3, sm=6, className="mb-3"),
        ], className="g-3 mb-4"),
        
        # Publication and Citation Trends
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Publication Trends", className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dcc.Graph(figure=fig_publications, config={'displayModeBar': False})
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Citation Trends", className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dcc.Graph(figure=fig_citations, config={'displayModeBar': False})
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),
        ]),
        
        # Top Cited Papers
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="bi bi-trophy-fill me-2"),
                            "Top 10 Most Cited Papers"
                        ], className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dag.AgGrid(
                            rowData=df_top_cited.to_dict("records") if not df_top_cited.empty else [],
                            columnDefs=[
                                {
                                    "field": "title",
                                    "headerName": "Title",
                                    "flex": 3,
                                    "wrapText": True,
                                    "autoHeight": True,
                                },
                                {
                                    "field": "publication_year",
                                    "headerName": "Year",
                                    "width": 90,
                                    "type": "numericColumn",
                                },
                                {
                                    "field": "cited_by_count",
                                    "headerName": "Citations",
                                    "width": 110,
                                    "type": "numericColumn",
                                    "cellStyle": {"fontWeight": "bold", "color": "#198754"},
                                },
                                {
                                    "field": "source_name",
                                    "headerName": "Source",
                                    "flex": 2,
                                },
                            ],
                            defaultColDef={
                                "resizable": True,
                                "sortable": True,
                                "filter": True,
                            },
                            dashGridOptions={
                                "pagination": False,
                                "domLayout": "autoHeight",
                            },
                            style={"width": "100%"},
                        ) if not df_top_cited.empty else html.P("No papers found", className="text-muted")
                    ], className="p-3")
                ], className="shadow-sm mb-4")
            ], width=12)
        ]),
        
        # Research Areas and Keywords
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Research Areas", className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dcc.Graph(figure=fig_subjects, config={'displayModeBar': False}) if not df_subjects.empty else html.P("No subject areas found", className="text-muted")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5("Research Keywords", className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dcc.Graph(figure=fig_keywords, config={'displayModeBar': False}) if not df_keywords.empty else html.P("No keywords found", className="text-muted")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),
        ]),
        
        # Collaborations and Sources
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="bi bi-people-fill me-2"),
                            "Top 10 Collaborators"
                        ], className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dag.AgGrid(
                            rowData=df_collaborators.to_dict("records") if not df_collaborators.empty else [],
                            columnDefs=[
                                {
                                    "field": "indexed_name",
                                    "headerName": "Collaborator",
                                    "flex": 2,
                                },
                                {
                                    "field": "collaboration_count",
                                    "headerName": "Joint Papers",
                                    "width": 140,
                                    "type": "numericColumn",
                                    "cellStyle": {"fontWeight": "bold", "color": "#0d6efd"},
                                },
                            ],
                            defaultColDef={
                                "resizable": True,
                                "sortable": True,
                                "filter": True,
                            },
                            dashGridOptions={
                                "pagination": False,
                                "domLayout": "autoHeight",
                            },
                            style={"width": "100%"},
                        ) if not df_collaborators.empty else html.P("No collaborators found", className="text-muted")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="bi bi-journal-text me-2"),
                            "Top Publication Venues"
                        ], className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dag.AgGrid(
                            rowData=df_sources.to_dict("records") if not df_sources.empty else [],
                            columnDefs=[
                                {
                                    "field": "source_name",
                                    "headerName": "Source",
                                    "flex": 3,
                                },
                                {
                                    "field": "paper_count",
                                    "headerName": "Papers",
                                    "width": 110,
                                    "type": "numericColumn",
                                    "cellStyle": {"fontWeight": "bold"},
                                },
                            ],
                            defaultColDef={
                                "resizable": True,
                                "sortable": True,
                                "filter": True,
                            },
                            dashGridOptions={
                                "pagination": False,
                                "domLayout": "autoHeight",
                            },
                            style={"width": "100%"},
                        ) if not df_sources.empty else html.P("No sources found", className="text-muted")
                    ])
                ], className="shadow-sm")
            ], md=6, className="mb-4"),
        ]),
        
        # Affiliations
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="bi bi-building me-2"),
                            "Institutional Affiliations"
                        ], className="mb-0 fw-semibold")
                    ]),
                    dbc.CardBody([
                        dag.AgGrid(
                            rowData=df_affiliations.to_dict("records") if not df_affiliations.empty else [],
                            columnDefs=[
                                {
                                    "field": "affiliation_name",
                                    "headerName": "Institution",
                                    "flex": 3,
                                },
                                {
                                    "field": "country",
                                    "headerName": "Country",
                                    "flex": 1,
                                },
                                {
                                    "field": "paper_count",
                                    "headerName": "Papers",
                                    "width": 110,
                                    "type": "numericColumn",
                                    "cellStyle": {"fontWeight": "bold"},
                                },
                            ],
                            defaultColDef={
                                "resizable": True,
                                "sortable": True,
                                "filter": True,
                            },
                            dashGridOptions={
                                "pagination": False,
                                "domLayout": "autoHeight",
                            },
                            style={"width": "100%"},
                        ) if not df_affiliations.empty else html.P("No affiliations found", className="text-muted")
                    ])
                ], className="shadow-sm mb-4")
            ], width=12)
        ]),
        
    ], fluid=True, className="py-4 px-3 px-md-4")