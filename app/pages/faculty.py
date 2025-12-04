from dash import Dash, html, dcc, callback, Input, Output, State
import dash_ag_grid as dag
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import text

from database import engine 

dash.register_page(__name__)



def get_authors(search_query="", affiliation_filter="chula_only", min_papers=0, min_citations=0):
    """Fetch authors based on search and filters."""
    try:
        
        # Build the WHERE clause based on filters
        where_conditions = []
        params = {}
        
        if search_query:
            where_conditions.append("LOWER(a.indexed_name) LIKE LOWER(:search)")
            params['search'] = f"%{search_query}%"
        
        if affiliation_filter == "chula_only":
            where_conditions.append("af.affiliation_id = 2")
        elif affiliation_filter == "thailand":
            where_conditions.append("af.country = 'Thailand'")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = text(f"""
            SELECT 
                a.author_id,
                a.indexed_name,
                a.auid,
                COUNT(DISTINCT pa.paper_id) AS paper_count,
                COALESCE(SUM(p.cited_by_count), 0) AS total_citations,
                STRING_AGG(DISTINCT af.affiliation_name, '; ' ORDER BY af.affiliation_name) AS affiliations
            FROM authors a
            JOIN paper_authors pa ON a.author_id = pa.author_id
            JOIN papers p ON pa.paper_id = p.paper_id
            JOIN paper_author_affiliations paa ON pa.paper_author_id = paa.paper_author_id
            JOIN affiliations af ON paa.affiliation_id = af.affiliation_id
            WHERE {where_clause}
            GROUP BY a.author_id, a.indexed_name, a.auid
            HAVING COUNT(DISTINCT pa.paper_id) >= :min_papers 
                AND COALESCE(SUM(p.cited_by_count), 0) >= :min_citations
            ORDER BY paper_count DESC, total_citations DESC
            LIMIT 100
        """)
        
        params['min_papers'] = min_papers
        params['min_citations'] = min_citations
        
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params=params)
            df['author_display'] = df.apply(
                lambda x: f'<a href="/author/{x["author_id"]}" style="text-decoration: none; color: #0d6efd; font-weight: 500;">{x["indexed_name"]}</a>',
                axis=1
            )
        return df
    except Exception as e:
        print(f"Error fetching authors: {e}")
        return pd.DataFrame()


def get_author_papers(author_id):
    """Fetch papers for a specific author."""
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
            ORDER BY p.publication_year DESC, p.cited_by_count DESC
        """)
        
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'author_id': author_id})
        
        if not df.empty:
            df["title"] = df["title"].apply(
                lambda x: x[:100] + "..." if x and len(str(x)) > 100 else x
            )
            df["all_authors"] = df["all_authors"].apply(
                lambda x: x[:80] + "..." if x and len(str(x)) > 80 else x
            )
        
        return df
    except Exception as e:
        print(f"Error fetching author papers: {e}")
        return pd.DataFrame()


# Initial data load
initial_df = get_authors(affiliation_filter="chula_only")

layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("Faculty & Author Directory", className="display-5 fw-bold text-center text-dark mt-4"),
            html.P(
                "Search and explore research authors and their publications",
                className="text-center text-muted mb-4"
            ),
            html.Hr(),
        ], width=12)
    ]),
    
    # Search and Filters Card
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("Search & Filters", className="mb-0 fw-semibold")
                ]),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Search by Author Name", className="fw-semibold"),
                            dbc.Input(
                                id="author-search-input",
                                type="text",
                                placeholder="Enter author name...",
                                debounce=True,
                                className="mb-3"
                            ),
                        ], md=6),
                        dbc.Col([
                            dbc.Label("Affiliation Filter", className="fw-semibold"),
                            dcc.Dropdown(
                                id="affiliation-filter",
                                options=[
                                    {"label": "Chulalongkorn University Only", "value": "chula_only"},
                                    {"label": "Thailand Universities", "value": "thailand"},
                                    {"label": "All Institutions", "value": "all"}
                                ],
                                value="chula_only",
                                clearable=False,
                                className="mb-3"
                            ),
                        ], md=6),
                    ]),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Minimum Papers", className="fw-semibold"),
                            dbc.Input(
                                id="min-papers-input",
                                type="number",
                                min=0,
                                value=0,
                                className="mb-3"
                            ),
                        ], md=6),
                        dbc.Col([
                            dbc.Label("Minimum Citations", className="fw-semibold"),
                            dbc.Input(
                                id="min-citations-input",
                                type="number",
                                min=0,
                                value=0,
                                className="mb-3"
                            ),
                        ], md=6),
                    ]),
                    dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                "Search",
                                id="search-button",
                                color="primary",
                                className="w-100",
                                size="lg"
                            ),
                        ], md=12),
                    ]),
                ]),
            ], className="shadow-sm mb-4"),
        ], width=12),
    ]),
    
    # Results Summary
    dbc.Row([
        dbc.Col([
            html.Div(id="results-summary", className="mb-3"),
        ], width=12),
    ]),
    
    # Authors Table
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("Authors", className="mb-0 fw-semibold")
                ]),
                dbc.CardBody([
                    html.Div(id="authors-table-container"),
                ], className="p-0"),
            ], className="shadow-sm mb-4"),
        ], width=12),
    ]),
    
    # Selected Author Papers
    dbc.Row([
        dbc.Col([
            html.Div(id="author-papers-section"),
        ], width=12),
    ]),
    
], fluid=True, className="py-4 px-3 px-md-4")


@callback(
    [Output("authors-table-container", "children"),
     Output("results-summary", "children")],
    [Input("search-button", "n_clicks")],
    [State("author-search-input", "value"),
     State("affiliation-filter", "value"),
     State("min-papers-input", "value"),
     State("min-citations-input", "value")]
)
def update_authors_table(n_clicks, search_query, affiliation_filter, min_papers, min_citations):
    """Update authors table based on search and filters."""
    search_query = search_query or ""
    min_papers = min_papers or 0
    min_citations = min_citations or 0
    
    df = get_authors(search_query, affiliation_filter, min_papers, min_citations)
    
    if df.empty:
        return [
            html.P("No authors found matching your criteria.", className="text-muted text-center p-4"),
            dbc.Alert("No results found. Try adjusting your filters.", color="warning", className="mb-3")
        ]
    
    # Create AG Grid table
    table = dag.AgGrid(
        id="authors-grid",
        rowData=df.to_dict("records"),
        columnDefs=[
            {
                "field": "author_display",
                "headerName": "Author Name",
                "flex": 2,
                "cellStyle": {"fontWeight": "500"},
                "cellRenderer": "markdown"
            },
            {
                "field": "affiliations",
                "headerName": "Affiliations",
                "flex": 3,
                "wrapText": True,
                "autoHeight": True,
            },
            {
                "field": "paper_count",
                "headerName": "Papers",
                "width": 110,
                "type": "numericColumn",
                "cellStyle": {"fontWeight": "bold", "color": "#0d6efd"},
            },
            {
                "field": "total_citations",
                "headerName": "Citations",
                "width": 120,
                "type": "numericColumn",
                "cellStyle": {"fontWeight": "bold", "color": "#198754"},
            },
            {
                "field": "auid",
                "headerName": "Scopus ID",
                "width": 130,
            },
        ],
        defaultColDef={
            "resizable": True,
            "sortable": True,
            "filter": True,
        },
        dashGridOptions={
            "pagination": True,
            "paginationPageSize": 20,
            "rowSelection": "single",
        },
        dangerously_allow_code=True,
        style={"height": "600px", "width": "100%"},
    )
    
    summary = dbc.Alert([
        html.I(className="bi bi-check-circle-fill me-2"),
        f"Found {len(df)} authors matching your criteria"
    ], color="success", className="mb-3")
    
    return [table, summary]