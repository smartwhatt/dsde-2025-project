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
                "field": "title_display",
                "headerName": "Title",
                "flex": 3,
                "minWidth": 300,
                "cellRenderer": "markdown",
            },
            {
                "field": "authors_display",
                "headerName": "Author",
                "width": 180,
                "tooltipField": "all_authors_tooltip",
            },
            {
                "field": "affiliations_display",
                "headerName": "Affiliations",
                "flex": 2,
                "minWidth": 250,
                "cellRenderer": "markdown",
                "wrapText": True,
                "autoHeight": True,
            },
            {
                "field": "keywords_display",
                "headerName": "Keywords",
                "flex": 2,
                "minWidth": 250,
                "cellRenderer": "markdown",
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
        dangerously_allow_code=True,
        dashGridOptions={
            "pagination": True,
            "paginationPageSize": 20,
            "domLayout": "autoHeight",
            "rowHeight": 60,
        },
        style={"height": "100%", "width": "100%"},
    )


def format_badges_html(items_str, selected_items=None):
    """Helper to generate HTML badges for items (affiliations or keywords)."""
    if not items_str:
        return ""
    
    items = [item.strip() for item in str(items_str).split(',')]
    html_parts = []
    limit = 4
    
    # Separate selected and non-selected items
    selected = []
    non_selected = []
    
    if selected_items:
        selected_set = set(selected_items)
        for item in items:
            if item in selected_set:
                selected.append(item)
            else:
                non_selected.append(item)
    else:
        non_selected = items
    
    # Display selected items first with primary color
    displayed_items = selected + non_selected
    
    for i, item in enumerate(displayed_items[:limit]):
        if selected_items and item in selected_items:
            # Highlight selected affiliations
            html_parts.append(
                f'<span class="badge rounded-pill bg-primary text-white border me-1" style="font-weight: 500;">{item}</span>'
            )
        else:
            html_parts.append(
                f'<span class="badge rounded-pill bg-light text-dark border me-1" style="font-weight: normal;">{item}</span>'
            )
    
    # Add counter badge if there are more items
    if len(displayed_items) > limit:
        html_parts.append(
            f'<span class="badge rounded-pill bg-light text-muted border" style="font-size: 0.7em;">+{len(displayed_items) - limit}</span>'
        )
    
    return "".join(html_parts)


def get_all_affiliations():
    """Fetch all unique affiliations for dropdown, alphabetically."""
    try:
        engine = create_engine(conn_string)
        # Modified query: Removed the JOIN and HAVING count > 0 to ensure all institutions show up
        query = text("""
            SELECT DISTINCT
                affiliation_name,
                country
            FROM affiliations
            WHERE affiliation_name IS NOT NULL
            ORDER BY affiliation_name ASC
        """)
        
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        
        # Create options for dropdown
        options = [
            {
                "label": f"{row['affiliation_name']} ({row['country']})" if row['country'] else row['affiliation_name'],
                "value": row['affiliation_name']
            }
            for _, row in df.iterrows()
        ]
        
        return options
    except Exception as e:
        print(f"Error fetching affiliations: {e}")
        return []


def get_papers_by_affiliations(selected_affiliations, min_citations=0, min_year=None, max_year=None):
    """Fetch papers associated with selected affiliations."""
    if not selected_affiliations:
        return pd.DataFrame()
    
    try:
        engine = create_engine(conn_string)
        
        where_conditions = []
        params = {}
        
        # Affiliation filter
        # We need to dynamically create parameters for the IN clause
        placeholders = ','.join([f':aff_{i}' for i in range(len(selected_affiliations))])
        
        # IMPORTANT: Use subquery to ensure we get papers that have ANY of the selected affiliations
        where_conditions.append(f"""
            EXISTS (
                SELECT 1 
                FROM paper_authors pa_sub
                JOIN paper_author_affiliations paa_sub ON pa_sub.paper_author_id = paa_sub.paper_author_id
                JOIN affiliations af_sub ON paa_sub.affiliation_id = af_sub.affiliation_id
                WHERE pa_sub.paper_id = p.paper_id
                AND af_sub.affiliation_name IN ({placeholders})
            )
        """)
        
        for i, aff in enumerate(selected_affiliations):
            params[f'aff_{i}'] = aff
        
        # Year filters
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
                -- Count total authors
                (SELECT COUNT(*) FROM paper_authors pa WHERE pa.paper_id = p.paper_id) as author_count,
                -- Get Keywords
                (
                    SELECT STRING_AGG(DISTINCT k.keyword, ',')
                    FROM paper_keywords pk
                    JOIN keywords k ON pk.keyword_id = k.keyword_id
                    WHERE pk.paper_id = p.paper_id
                ) as keywords_list,
                -- Get all affiliations for this paper (for display)
                (
                    SELECT STRING_AGG(DISTINCT af_all.affiliation_name, ',')
                    FROM paper_authors pa_all
                    JOIN paper_author_affiliations paa_all ON pa_all.paper_author_id = paa_all.paper_author_id
                    JOIN affiliations af_all ON paa_all.affiliation_id = af_all.affiliation_id
                    WHERE pa_all.paper_id = p.paper_id
                ) as all_affiliations,
                -- Full author list for tooltip
                (
                    SELECT STRING_AGG(DISTINCT a_all.indexed_name, ', ')
                    FROM paper_authors pa_all
                    JOIN authors a_all ON pa_all.author_id = a_all.author_id
                    WHERE pa_all.paper_id = p.paper_id
                ) as all_authors_full
            FROM papers p
            WHERE {where_clause}
            ORDER BY p.publication_year DESC, p.cited_by_count DESC
            LIMIT 500
        """)
        
        params['min_citations'] = int(min_citations) if min_citations else 0
        
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params=params)
        
        if not df.empty:
            # Format Author Column
            df['authors_display'] = df.apply(
                lambda x: f"{x['first_author']} et al." if x['author_count'] > 1 else x['first_author'],
                axis=1
            ).fillna("Unknown")
            
            # Format Title as HTML Hyperlink
            df['title_display'] = df.apply(
                lambda x: f'<a href="/papers/{x["paper_id"]}" style="text-decoration: none; color: #0d6efd; font-weight: 500;">{x["title"]}</a>',
                axis=1
            )
            
            # Format Affiliations as HTML Badges (selected ones first)
            df['affiliations_display'] = df['all_affiliations'].apply(
                lambda x: format_badges_html(x, selected_affiliations)
            )
            
            # Format Keywords as HTML Badges
            df['keywords_display'] = df['keywords_list'].apply(
                lambda x: format_badges_html(x)
            )
            
            # Tooltip
            df['all_authors_tooltip'] = df['all_authors_full']
        
        return df
    except Exception as e:
        print(f"Error fetching papers by affiliations: {e}")
        return pd.DataFrame()


# Get all affiliations for dropdown
affiliation_options = get_all_affiliations()

layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("Papers by Affiliation", className="display-5 fw-bold text-center text-dark mt-4"),
            html.P(
                "Search and explore papers from specific institutions and affiliations",
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
                        # Affiliation Selection
                        dbc.Col([
                            dbc.Label("Select Affiliations", className="fw-semibold"),
                            dcc.Dropdown(
                                id="affiliation-select",
                                options=affiliation_options,
                                multi=True,
                                placeholder="Search and select institutions...",
                                className="mb-3",
                                style={"minHeight": "42px"} # Basic height fix
                            ),
                        ], md=12),
                    ]),
                    
                    dbc.Row([
                        # Filters
                        dbc.Col([
                            dbc.Label("Min Citations", className="fw-semibold"),
                            dbc.Input(
                                id="aff-min-citations-input",
                                type="number",
                                min=0,
                                placeholder="0",
                                className="mb-3"
                            ),
                        ], md=4),
                        dbc.Col([
                            dbc.Label("Min Year", className="fw-semibold"),
                            dbc.Input(
                                id="aff-min-year-input",
                                type="number",
                                min=1900,
                                placeholder="1900",
                                className="mb-3"
                            ),
                        ], md=4),
                        dbc.Col([
                            dbc.Label("Max Year", className="fw-semibold"),
                            dbc.Input(
                                id="aff-max-year-input",
                                type="number",
                                min=1900,
                                placeholder="2025",
                                className="mb-3"
                            ),
                        ], md=4),
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                [html.I(className="bi bi-search me-2"), "Search Papers"],
                                id="aff-search-button",
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
            html.Div(id="aff-results-summary", className="mb-3"),
        ], width=12),
    ]),
    
    # Main Table
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(id="aff-papers-table-container"),
                ], className="p-0"),
            ], className="shadow-sm mb-4 border-0"),
        ], width=12),
    ]),
    
], fluid=True, className="py-4 px-3 px-md-4")


@callback(
    [Output("aff-papers-table-container", "children"),
     Output("aff-results-summary", "children")],
    [Input("aff-search-button", "n_clicks")],
    [State("affiliation-select", "value"),
     State("aff-min-citations-input", "value"),
     State("aff-min-year-input", "value"),
     State("aff-max-year-input", "value")]
)
def update_papers_table(n_clicks, selected_affiliations, min_citations, min_year, max_year):
    """Update papers table based on selected affiliations and filters."""
    if not selected_affiliations:
        return [
            html.Div([
                html.I(className="bi bi-building display-4 text-muted mb-3"),
                html.H5("No affiliations selected", className="text-muted"),
                html.P("Please select one or more institutions from the dropdown above.", className="text-muted")
            ], className="text-center p-5"),
            dbc.Alert("Please select at least one affiliation to search.", color="info", className="mb-3")
        ]
    
    min_citations = min_citations or 0
    min_year = min_year if min_year not in (None, "") else None
    max_year = max_year if max_year not in (None, "") else None
    
    df = get_papers_by_affiliations(selected_affiliations, min_citations, min_year, max_year)
    
    if df.empty:
        return [
            html.Div([
                html.I(className="bi bi-exclamation-circle display-4 text-muted mb-3"),
                html.H5("No papers found", className="text-muted"),
                html.P("Try adjusting your filters or selecting different affiliations.", className="text-muted")
            ], className="text-center p-5"),
            dbc.Alert("No results found.", color="warning", className="mb-3")
        ]
    
    table = create_papers_grid(df, "aff-papers-grid")
    
    selected_text = ", ".join(selected_affiliations[:3])
    if len(selected_affiliations) > 3:
        selected_text += f" and {len(selected_affiliations) - 3} more"
    
    summary = dbc.Alert([
        html.I(className="bi bi-check-circle-fill me-2"),
        f"Found {len(df):,} papers from: {selected_text}"
    ], color="success", className="mb-3")
    
    return [table, summary]