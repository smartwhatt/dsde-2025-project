from dash import Dash, html, dcc, callback, Input, Output, State
import dash_ag_grid as dag
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import create_engine, text
import dotenv

dash.register_page(__name__)

conn_string = dotenv.get_key(".env", "CONN_STRING")

def create_papers_grid(df, grid_id):
    """Helper function to create the Dash AG Grid component."""
    return dag.AgGrid(
        id=grid_id,
        rowData=df.to_dict("records"),
        columnDefs=[
            {
                "field": "title_display", # Uses the pre-formatted HTML column
                "headerName": "Title",
                "flex": 3,
                "minWidth": 300,
                "cellRenderer": "markdown", # Use markdown/html renderer
            },
            {
                "field": "authors_display",
                "headerName": "Author",
                "width": 180,
                "tooltipField": "all_authors_tooltip",
            },
            {
                "field": "keywords_display", # Uses the pre-formatted HTML column
                "headerName": "Keywords",
                "flex": 2,
                "minWidth": 250,
                "cellRenderer": "markdown", # Use markdown/html renderer
                "wrapText": True,
                "autoHeight": True,
            },
            {
                "field": "publication_year",
                "headerName": "Year",
                "width": 100,
                "type": "numericColumn",
            },
            {
                "field": "cited_by_count",
                "headerName": "Citations",
                "width": 110,
                "type": "numericColumn",
                "cellStyle": {"fontWeight": "bold", "color": "#198754"},
            },
        ],
        defaultColDef={
            "resizable": True,
            "sortable": True,
            "filter": True,
        },
        columnSize="sizeToFit",
        # FIX: Use the correct property name allowed in your version
        dangerously_allow_code=True, 
        dashGridOptions={
            "pagination": True,
            "paginationPageSize": 20,
            "domLayout": "autoHeight",
            "rowHeight": 60,
        },
        style={"height": "100%", "width": "100%"},
    )


def format_keywords_html(keywords_str):
    """Python helper to generate HTML badges for keywords."""
    if not keywords_str: 
        return ""
    
    # Split keywords
    keywords = [k.strip() for k in str(keywords_str).split(',')]
    html_parts = []
    limit = 4 
    
    # Create pills for the first few keywords
    for k in keywords[:limit]:
        # Using Bootstrap badge classes directly in the string
        html_parts.append(
            f'<span class="badge rounded-pill bg-light text-dark border me-1" style="font-weight: normal;">{k}</span>'
        )
    
    # Add a counter badge if there are more
    if len(keywords) > limit:
        html_parts.append(
            f'<span class="badge rounded-pill bg-light text-muted border" style="font-size: 0.7em;">+{len(keywords) - limit}</span>'
        )
        
    return "".join(html_parts)


def get_papers(search_query="", min_citations=0, min_year=None, max_year=None):
    """Fetch papers based on unified search and filters."""
    try:
        engine = create_engine(conn_string)
        
        where_conditions = []
        params = {}
        
        # 1. Unified Search Logic
        if search_query:
            where_conditions.append(
                """
                (
                    LOWER(p.title) LIKE LOWER(:search) OR 
                    LOWER(p.abstract) LIKE LOWER(:search) OR
                    EXISTS (
                        SELECT 1 FROM paper_authors pa 
                        JOIN authors a ON pa.author_id = a.author_id
                        WHERE pa.paper_id = p.paper_id 
                        AND LOWER(a.indexed_name) LIKE LOWER(:search)
                    ) OR
                    EXISTS (
                        SELECT 1 FROM paper_keywords pk 
                        JOIN keywords k ON pk.keyword_id = k.keyword_id
                        WHERE pk.paper_id = p.paper_id 
                        AND LOWER(k.keyword) LIKE LOWER(:search)
                    )
                )
                """
            )
            params['search'] = f"%{search_query}%"
        
        # 2. Numeric Filters
        if min_year is not None:
            where_conditions.append("p.publication_year >= :min_year")
            params['min_year'] = int(min_year) if min_year else 0

        if max_year is not None:
            where_conditions.append("p.publication_year <= :max_year")
            params['max_year'] = int(max_year) if max_year else 2100
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = text(f"""
            SELECT 
                p.paper_id,
                p.title,
                p.publication_year,
                p.cited_by_count,
                -- Get First Author Name
                (
                    SELECT a.indexed_name 
                    FROM paper_authors pa 
                    JOIN authors a ON pa.author_id = a.author_id 
                    WHERE pa.paper_id = p.paper_id 
                    ORDER BY pa.author_sequence ASC 
                    LIMIT 1
                ) as first_author,
                -- Count total authors to determine 'et al.'
                (SELECT COUNT(*) FROM paper_authors pa WHERE pa.paper_id = p.paper_id) as author_count,
                -- Get Keywords for display
                STRING_AGG(DISTINCT k.keyword, ',') as keywords_list,
                -- Full author list for tooltip
                STRING_AGG(DISTINCT a_all.indexed_name, ', ') as all_authors_full
            FROM papers p
            LEFT JOIN paper_keywords pk ON p.paper_id = pk.paper_id
            LEFT JOIN keywords k ON pk.keyword_id = k.keyword_id
            LEFT JOIN paper_authors pa_join ON p.paper_id = pa_join.paper_id
            LEFT JOIN authors a_all ON pa_join.author_id = a_all.author_id
            WHERE {where_clause}
            GROUP BY p.paper_id, p.title, p.publication_year, p.cited_by_count
            HAVING COALESCE(p.cited_by_count, 0) >= :min_citations
            ORDER BY p.publication_year DESC, p.cited_by_count DESC
        """)
        
        params['min_citations'] = int(min_citations) if min_citations else 0
        
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params=params)
        
        if not df.empty:
            # Format Author Column: "First Author et al."
            df['authors_display'] = df.apply(
                lambda x: f"{x['first_author']} et al." if x['author_count'] > 1 else x['first_author'], 
                axis=1
            ).fillna("Unknown")
            
            # Format Title as HTML Hyperlink
            df['title_display'] = df.apply(
                lambda x: f'<a href="/papers/{x["paper_id"]}" style="text-decoration: none; color: #0d6efd; font-weight: 500;">{x["title"]}</a>',
                axis=1
            )
            
            # Format Keywords as HTML Badges
            df['keywords_display'] = df['keywords_list'].apply(format_keywords_html)
            
            # Tooltip
            df['all_authors_tooltip'] = df['all_authors_full']

        return df
    except Exception as e:
        print(f"Error fetching papers: {e}")
        return pd.DataFrame()


# Initial Load
initial_df = get_papers() 

layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("Research Paper Directory", className="display-5 fw-bold text-center text-dark mt-4"),
            html.P(
                "Explore publications by title, author, abstract, or topic keywords",
                className="text-center text-muted mb-4"
            ),
            html.Hr(),
        ], width=12)
    ]),
    
    # Search & Filter Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        # Single Unified Search Box
                        dbc.Col([
                            dbc.Label("Search", className="fw-semibold"),
                            dbc.Input(
                                id="paper-search-input",
                                type="text",
                                placeholder="Search by Title, Abstract, Author, or Keyword...",
                                debounce=True,
                                className="mb-3"
                            ),
                        ], md=6), # Larger search box
                        
                        # Filters (Year & Citations)
                        dbc.Col([
                            dbc.Label("Min Citations", className="fw-semibold"),
                            dbc.Input(id="min-citations-input", type="number", min=0, placeholder="0", className="mb-3"),
                        ], md=2),
                        dbc.Col([
                            dbc.Label("Min Year", className="fw-semibold"),
                            dbc.Input(id="min-year-input", type="number", min=1900, placeholder="1900", className="mb-3"),
                        ], md=2),
                        dbc.Col([
                            dbc.Label("Max Year", className="fw-semibold"),
                            dbc.Input(id="max-year-input", type="number", min=1900, placeholder="2025", className="mb-3"),
                        ], md=2),
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                [html.I(className="bi bi-search me-2"), "Search Papers"],
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
    
    # Results Feedback
    dbc.Row([
        dbc.Col([
            html.Div(id="results-summary", className="mb-3"),
        ], width=12),
    ]),
    
    # Main Table
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(
                        create_papers_grid(initial_df, "papers-grid"),
                        id="papers-table-container"
                    ),
                ], className="p-0"),
            ], className="shadow-sm mb-4 border-0"),
        ], width=12),
    ]),
    
], fluid=True, className="py-4 px-3 px-md-4")


@callback(
    [Output("papers-table-container", "children", allow_duplicate=True),
     Output("results-summary", "children", allow_duplicate=True)],
    [Input("search-button", "n_clicks")],
    [State("paper-search-input", "value"),
     State("min-citations-input", "value"),
     State("min-year-input", "value"),
     State("max-year-input", "value")],
     prevent_initial_call=True
)
def update_papers_table(n_clicks, search_query, min_citations, min_year, max_year):
    """Update papers table based on unified search and filters."""
    search_query = search_query or ""
    min_citations = min_citations or 0
    min_year = min_year if min_year not in (None, "") else None
    max_year = max_year if max_year not in (None, "") else None
    
    df = get_papers(search_query, min_citations, min_year, max_year)
    
    if df.empty:
        return [
            html.Div([
                html.I(className="bi bi-exclamation-circle display-4 text-muted mb-3"),
                html.H5("No papers found", className="text-muted"),
                html.P("Try adjusting your search terms or filters.", className="text-muted")
            ], className="text-center p-5"),
            dbc.Alert("No results found.", color="warning", className="mb-3")
        ]
    
    table = create_papers_grid(df, "papers-grid")
    
    summary = dbc.Alert([
        html.I(className="bi bi-check-circle-fill me-2"),
        f"Found {len(df):,} papers matching your criteria"
    ], color="success", className="mb-3")
    
    return [table, summary]