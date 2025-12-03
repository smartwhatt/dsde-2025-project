from dash import html, dcc, callback, Input, Output, State
import dash
import dash_bootstrap_components as dbc

import pandas as pd
from sqlalchemy import create_engine, text
import dotenv
import math

dash.register_page(__name__, name="Papers")

# ------------------------------------------------------------------------------
# Database setup
# ------------------------------------------------------------------------------

# Read PostgreSQL connection string from .env
CONN_STRING = dotenv.get_key(".env", "CONN_STRING")

# Create a SQLAlchemy engine
engine = create_engine(CONN_STRING)

PAGE_SIZE = 50  # 1 page contains 50 papers


def fetch_papers(search_query: str = "") -> pd.DataFrame:
    """
    Fetch papers with first author, author count, aggregated keywords, and year.
    Optionally filter by a free-text search across title, first author, keywords, and year.
    """
    search_query = (search_query or "").strip()
    params = {}

    base_query = """
        SELECT
            p.paper_id,
            p.title,
            p.publication_year,
            COALESCE(fa.indexed_name, 'Unknown author') AS first_author,
            COALESCE(pac.author_count, 0) AS author_count,
            kagg.keywords
        FROM papers p
        LEFT JOIN (
            SELECT 
                paper_id,
                COUNT(*) AS author_count,
                MIN(CASE WHEN author_sequence = 1 THEN author_id END) AS first_author_id
            FROM paper_authors
            GROUP BY paper_id
        ) pac ON pac.paper_id = p.paper_id
        LEFT JOIN authors fa ON fa.author_id = pac.first_author_id
        LEFT JOIN (
            SELECT 
                pk.paper_id,
                STRING_AGG(DISTINCT k.keyword, ', ' ORDER BY k.keyword) AS keywords
            FROM paper_keywords pk
            JOIN keywords k ON k.keyword_id = pk.keyword_id
            GROUP BY pk.paper_id
        ) kagg ON kagg.paper_id = p.paper_id
        WHERE 1=1
    """

    if search_query:
        base_query += """
            AND (
                LOWER(p.title) LIKE :search
                OR LOWER(COALESCE(fa.indexed_name, '')) LIKE :search
                OR EXISTS (
                    SELECT 1
                    FROM paper_keywords pk2
                    JOIN keywords k2 ON k2.keyword_id = pk2.keyword_id
                    WHERE pk2.paper_id = p.paper_id
                      AND LOWER(k2.keyword) LIKE :search
                )
                OR CAST(p.publication_year AS TEXT) LIKE :search
            )
        """
        params["search"] = f"%{search_query.lower()}%"

    # Order by most recent year first, then title
    base_query += """
        ORDER BY p.publication_year DESC NULLS LAST, p.title ASC
    """

    with engine.connect() as connection:
        df = pd.read_sql_query(text(base_query), connection, params=params)

    return df


def make_keywords_chips(keyword_str: str | None) -> html.Div:
    """
    Render keywords as small Bootstrap 'chips' (badges).
    """
    if not keyword_str:
        return html.Div("-", className="text-muted")

    keywords = [k.strip() for k in keyword_str.split(",") if k.strip()]
    if not keywords:
        return html.Div("-", className="text-muted")

    return html.Div(
        [
            dbc.Badge(
                kw,
                pill=True,
                color="secondary",
                className="me-1 mb-1 small",
            )
            for kw in keywords
        ],
        className="d-flex flex-wrap",
    )


def make_papers_table(df_page: pd.DataFrame) -> dbc.Table:
    """
    Create a Bootstrap table for the current page of papers.
    """
    if df_page.empty:
        return dbc.Table(
            html.Tbody(
                html.Tr(
                    html.Td(
                        "No papers found matching your query.",
                        colSpan=4,
                        className="text-center text-muted py-4",
                    )
                )
            ),
            bordered=False,
            hover=False,
            striped=False,
            className="mb-0",
        )

    rows = []
    for _, row in df_page.iterrows():
        first_author = row.get("first_author") or "Unknown author"
        author_count = row.get("author_count") or 0

        if author_count > 1:
            first_author_display = f"{first_author} et al."
        else:
            first_author_display = first_author

        title_link = html.A(
            row.get("title") or "(Untitled)",
            href=f"/papers/{row.get('paper_id')}",
            className="text-decoration-none fw-semibold",
        )

        keywords_cell = make_keywords_chips(row.get("keywords"))

        year = row.get("publication_year") or "-"

        rows.append(
            html.Tr(
                [
                    html.Td(first_author_display, className="align-middle"),
                    html.Td(
                        title_link,
                        className="align-middle",
                    ),
                    html.Td(
                        keywords_cell,
                        className="align-middle",
                    ),
                    html.Td(str(year), className="align-middle text-center"),
                ]
            )
        )

    return dbc.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("First Author", className="w-25"),
                        html.Th("Paper Title"),
                        html.Th("Keywords", className="w-25"),
                        html.Th("Year", className="text-center", style={"width": "80px"}),
                    ]
                )
            ),
            html.Tbody(rows),
        ],
        bordered=False,
        hover=True,
        striped=True,
        responsive=True,
        className="mb-0 align-middle",
    )


# ------------------------------------------------------------------------------
# Layout
# ------------------------------------------------------------------------------

layout = html.Div(
    [
        dcc.Store(id="papers-total-pages", data=1),
        html.H1("Papers", className="mb-4"),

        # Search bar
        dbc.Row(
            [
                dbc.Col(
                    dbc.InputGroup(
                        [
                            dbc.Input(
                                id="papers-search-input",
                                placeholder="Search by title, first author, keywords, or year…",
                                type="text",
                                debounce=True,
                            ),
                            dbc.Button(
                                "Search",
                                id="papers-search-button",
                                color="primary",
                                n_clicks=0,
                            ),
                        ]
                    ),
                    md=7,
                    xs=12,
                    className="mb-2",
                ),
                dbc.Col(
                    html.Small(
                        "Default: showing all papers (50 per page).",
                        className="text-muted",
                    ),
                    md=5,
                    xs=12,
                    className="d-flex align-items-center justify-content-md-end mb-2",
                ),
            ],
            className="mb-3",
        ),

        # Results summary
        html.Div(id="papers-results-summary", className="text-muted small mb-2"),

        # Table card
        dbc.Card(
            [
                dbc.CardBody(
                    html.Div(id="papers-table-container"),
                    className="p-0",
                )
            ],
            className="shadow-sm",
        ),

        # Pagination controls – bottom right
        html.Div(
            [
                html.Span("Pages:", className="me-2 small text-muted"),
                dbc.Button(
                    "1",
                    id="papers-page-btn-a",
                    size="sm",
                    outline=True,
                    color="secondary",
                    className="rounded-circle px-3 me-1",
                    n_clicks=0,
                ),
                dbc.Button(
                    "2",
                    id="papers-page-btn-b",
                    size="sm",
                    outline=True,
                    color="secondary",
                    className="rounded-circle px-3 me-1",
                    n_clicks=0,
                ),
                dbc.Button(
                    "3",
                    id="papers-page-btn-c",
                    size="sm",
                    outline=True,
                    color="secondary",
                    className="rounded-circle px-3 me-1",
                    n_clicks=0,
                ),
                html.Span("…", className="mx-1 text-muted"),

                dbc.Input(
                    id="papers-page-jump-input",
                    type="number",
                    min=1,
                    step=1,
                    placeholder="Page",
                    size="sm",
                    style={"width": "80px"},
                    className="me-1",
                ),
                dbc.Button(
                    "Go",
                    id="papers-page-jump-go",
                    size="sm",
                    outline=True,
                    color="secondary",
                    className="me-2",
                    n_clicks=0,
                ),
                dbc.Button(
                    "1",
                    id="papers-page-last-btn",
                    size="sm",
                    outline=True,
                    color="secondary",
                    className="rounded-circle px-3",
                    n_clicks=0,
                ),
            ],
            id="papers-pagination",
            className="d-flex justify-content-end align-items-center mt-3 flex-wrap gap-1",
        ),
    ],
    className="py-4 px-3 px-md-4",
)


# ------------------------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------------------------

@callback(
    [
        Output("papers-table-container", "children"),
        Output("papers-results-summary", "children"),
        Output("papers-total-pages", "data"),
        Output("papers-page-btn-a", "children"),
        Output("papers-page-btn-b", "children"),
        Output("papers-page-btn-c", "children"),
        Output("papers-page-last-btn", "children"),
        Output("papers-page-btn-a", "disabled"),
        Output("papers-page-btn-b", "disabled"),
        Output("papers-page-btn-c", "disabled"),
        Output("papers-page-last-btn", "disabled"),
        Output("papers-page-btn-a", "className"),
        Output("papers-page-btn-b", "className"),
        Output("papers-page-btn-c", "className"),
        Output("papers-page-last-btn", "className"),
    ],
    [
        Input("papers-search-button", "n_clicks"),
        Input("papers-search-input", "n_submit"),
        Input("papers-page-btn-a", "n_clicks"),
        Input("papers-page-btn-b", "n_clicks"),
        Input("papers-page-btn-c", "n_clicks"),
        Input("papers-page-last-btn", "n_clicks"),
        Input("papers-page-jump-go", "n_clicks"),
    ],
    [
        State("papers-search-input", "value"),
        State("papers-page-btn-a", "children"),
        State("papers-page-btn-b", "children"),
        State("papers-page-btn-c", "children"),
        State("papers-page-last-btn", "children"),
        State("papers-page-jump-input", "value"),
    ],
)
def update_papers_view(
    search_clicks,
    search_submit,
    page_a_clicks,
    page_b_clicks,
    page_c_clicks,
    page_last_clicks,
    jump_clicks,
    search_value,
    page_a_label,
    page_b_label,
    page_c_label,
    page_last_label,
    jump_value,
):
    """
    Single callback that:
      - performs the (DB-backed) search,
      - figures out the current page from what the user just clicked,
      - slices results to 50 papers,
      - renders the table,
      - and updates the pagination UI.
    """
    # Determine which input triggered this callback
    ctx = dash.callback_context
    if not ctx.triggered:
        triggered_id = "init"
    else:
        triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]

    search_query = (search_value or "").strip()

    # Fetch all matching papers
    df_all = fetch_papers(search_query)
    total_rows = len(df_all)
    total_pages = max(math.ceil(total_rows / PAGE_SIZE), 1)

    # Decide which page we're on, based on the trigger
    # Default: first page
    page = 1

    try:
        if triggered_id == "papers-page-btn-a":
            page = int(page_a_label or 1)
        elif triggered_id == "papers-page-btn-b":
            page = int(page_b_label or 1)
        elif triggered_id == "papers-page-btn-c":
            page = int(page_c_label or 1)
        elif triggered_id == "papers-page-last-btn":
            page = int(page_last_label or total_pages)
        elif triggered_id == "papers-page-jump-go":
            # Jump to page from input, clamped to valid range
            if jump_value is not None:
                page = int(jump_value)
        elif triggered_id in ("papers-search-button", "papers-search-input", "init"):
            # New search or first load: reset to page 1
            page = 1
    except (ValueError, TypeError):
        page = 1

    # Clamp page within valid bounds
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    # Paginate the DataFrame
    if total_rows == 0:
        df_page = df_all.iloc[0:0]
        start_idx = 0
        end_idx = 0
    else:
        start_idx = (page - 1) * PAGE_SIZE
        end_idx = min(start_idx + PAGE_SIZE, total_rows)
        df_page = df_all.iloc[start_idx:end_idx]

    table_component = make_papers_table(df_page)

    # Build summary text
    if total_rows == 0:
        summary = "No papers found."
    else:
        summary = f"Showing {start_idx + 1}–{end_idx} of {total_rows} papers (Page {page} of {total_pages})."

    # Decide which three page numbers to show (including current page)
    if total_pages <= 3:
        pages_triplet = list(range(1, total_pages + 1))
        while len(pages_triplet) < 3:
            pages_triplet.append(None)
    else:
        if page <= 2:
            pages_triplet = [1, 2, 3]
        elif page >= total_pages - 1:
            pages_triplet = [total_pages - 2, total_pages - 1, total_pages]
        else:
            pages_triplet = [page - 1, page, page + 1]

    a_value, b_value, c_value = pages_triplet
    last_value = total_pages

    # Button enable/disable + styling
    def button_props(page_number: int | None, is_current: bool):
        if page_number is None:
            return "", True, "d-none"  # label, disabled, className (hidden)

        base_class = "btn btn-sm rounded-circle px-3 me-1"
        if is_current:
            # Current page highlighted as light gray circle
            return str(page_number), False, base_class + " btn-secondary"
        else:
            return str(page_number), False, base_class + " btn-outline-secondary"

    # Triplet buttons
    a_label, a_disabled, a_class = button_props(a_value, a_value == page)
    b_label, b_disabled, b_class = button_props(b_value, b_value == page)
    c_label, c_disabled, c_class = button_props(c_value, c_value == page)

    # Last page button
    last_is_current = last_value == page
    last_label, last_disabled, last_class = button_props(last_value, last_is_current)

    return (
        table_component,
        summary,
        total_pages,
        a_label,
        b_label,
        c_label,
        last_label,
        a_disabled,
        b_disabled,
        c_disabled,
        last_disabled,
        a_class,
        b_class,
        c_class,
        last_class,
    )
