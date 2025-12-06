import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import pandas as pd
from sqlalchemy import text
import plotly.express as px
import plotly.graph_objects as go

from database import engine


dash.register_page(__name__, path="/")


def get_hero_stats():
    """Fetch overview statistics for hero section."""
    try:
        with engine.connect() as connection:
            query = text(
                """
                SELECT 
                    (SELECT COUNT(*) FROM papers) as total_papers,
                    (SELECT COUNT(DISTINCT auid) FROM authors) as total_authors,
                    (SELECT COUNT(*) FROM affiliations) as total_affiliations,
                    (SELECT SUM(cited_by_count) FROM papers) as total_citations,
                    (SELECT MIN(publication_year) FROM papers WHERE publication_year IS NOT NULL) as earliest_year,
                    (SELECT MAX(publication_year) FROM papers WHERE publication_year IS NOT NULL) as latest_year
            """
            )
            result = connection.execute(query).fetchone()
            return {
                "total_papers": result[0] or 0,
                "total_authors": result[1] or 0,
                "total_affiliations": result[2] or 0,
                "total_citations": result[3] or 0,
                "earliest_year": result[4],
                "latest_year": result[5],
            }
    except Exception as e:
        print(f"Error fetching hero stats: {e}")
        return {}


def get_publications_by_year():
    """Fetch publications trend by year."""
    try:
        query = text(
            """
            SELECT 
                publication_year,
                COUNT(*) as paper_count
            FROM papers
            WHERE publication_year IS NOT NULL
            GROUP BY publication_year
            ORDER BY publication_year
        """
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching publications by year: {e}")
        return pd.DataFrame()


def get_top_cited_papers():
    """Fetch top 10 most cited papers."""
    try:
        query = text(
            """
            SELECT 
                p.title,
                p.paper_id,
                p.publication_year,
                p.cited_by_count,
                s.source_name as journal,
                STRING_AGG(DISTINCT a.indexed_name, ', ' ORDER BY a.indexed_name) as authors
            FROM papers p
            LEFT JOIN sources s ON p.source_id = s.source_id
            LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
            LEFT JOIN authors a ON pa.author_id = a.author_id
            GROUP BY p.paper_id, p.title, p.publication_year, p.cited_by_count, s.source_name
            ORDER BY p.cited_by_count DESC
            LIMIT 10
        """
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        if not df.empty:
            df["title_display"] = df.apply(
                lambda x: f'<a href="/papers/{x["paper_id"]}" style="text-decoration: none; color: #0d6efd; font-weight: 500;">{x["title"][:80] + "..." if len(str(x["title"])) > 80 else x["title"]}</a>',
                axis=1,
            )

            df["authors"] = df["authors"].apply(
                lambda x: x[:60] + "..." if x and len(str(x)) > 60 else x
            )
        return df
    except Exception as e:
        print(f"Error fetching top cited papers: {e}")
        return pd.DataFrame()


def get_top_authors():
    """Fetch top 10 most prolific authors from Chulalongkorn."""
    try:
        query = text(
            """
            SELECT 
                    a.indexed_name,
                    COUNT(DISTINCT pa.paper_id) AS paper_count,
                    COALESCE(SUM(p.cited_by_count), 0) AS total_citations
                FROM authors a
                JOIN paper_authors pa 
                    ON a.author_id = pa.author_id
                JOIN papers p
                    ON pa.paper_id = p.paper_id        -- ensure correct join column name
                JOIN paper_author_affiliations paa
                    ON pa.paper_author_id = paa.paper_author_id
                JOIN affiliations af
                    ON paa.affiliation_id = af.affiliation_id
                WHERE af.affiliation_id = 2  -- Chulalongkorn University
                GROUP BY a.author_id, a.indexed_name
                ORDER BY paper_count DESC
                LIMIT 10;
        """
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching top authors: {e}")
        return pd.DataFrame()


def get_subject_areas():
    """Fetch papers by subject area."""
    try:
        query = text(
            """
            SELECT 
                sa.subject_name,
                COUNT(DISTINCT psa.paper_id) as paper_count
            FROM subject_areas sa
            JOIN paper_subject_areas psa ON sa.subject_area_id = psa.subject_area_id
            GROUP BY sa.subject_area_id, sa.subject_name
            ORDER BY paper_count DESC
            LIMIT 10
        """
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching subject areas: {e}")
        return pd.DataFrame()


def get_top_keywords():
    """Fetch top 15 keywords."""
    try:
        query = text(
            """
            SELECT 
                k.keyword,
                COUNT(DISTINCT pk.paper_id) as paper_count
            FROM keywords k
            JOIN paper_keywords pk ON k.keyword_id = pk.keyword_id
            GROUP BY k.keyword_id, k.keyword
            ORDER BY paper_count DESC
            LIMIT 15
        """
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching keywords: {e}")
        return pd.DataFrame()


def get_top_institutions():
    """Fetch top 10 institutions by paper output."""
    try:
        query = text(
            """
            SELECT 
                af.affiliation_name,
                af.country,
                COUNT(DISTINCT pa.paper_id) as paper_count
            FROM affiliations af
            JOIN paper_author_affiliations paa ON af.affiliation_id = paa.affiliation_id
            JOIN paper_authors pa ON paa.paper_author_id = pa.paper_author_id
            GROUP BY af.affiliation_id, af.affiliation_name, af.country
            ORDER BY paper_count DESC
            LIMIT 10
        """
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        if not df.empty:
            df["affiliation_name"] = df["affiliation_name"].apply(
                lambda x: x[:60] + "..." if x and len(str(x)) > 60 else x
            )
        return df
    except Exception as e:
        print(f"Error fetching top institutions: {e}")
        return pd.DataFrame()


def get_papers_by_country():
    """Fetch papers by country."""
    try:
        query = text(
            """
            SELECT 
                af.country,
                COUNT(DISTINCT pa.paper_id) as paper_count
            FROM affiliations af
            JOIN paper_author_affiliations paa ON af.affiliation_id = paa.affiliation_id
            JOIN paper_authors pa ON paa.paper_author_id = pa.paper_author_id
            WHERE af.country IS NOT NULL
            GROUP BY af.country
            ORDER BY paper_count DESC
            LIMIT 15
        """
        )
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching papers by country: {e}")
        return pd.DataFrame()


# Fetch all data
hero_stats = get_hero_stats()
df_publications = get_publications_by_year()
df_top_cited = get_top_cited_papers()
df_top_authors = get_top_authors()
df_subjects = get_subject_areas()
df_keywords = get_top_keywords()
df_institutions = get_top_institutions()
df_countries = get_papers_by_country()

# Create visualizations
fig_publications = (
    px.line(
        df_publications,
        x="publication_year",
        y="paper_count",
        title="Publications Over Time",
        labels={"publication_year": "Year", "paper_count": "Number of Papers"},
    )
    if not df_publications.empty
    else go.Figure()
)

fig_subjects = (
    px.bar(
        df_subjects,
        x="paper_count",
        y="subject_name",
        orientation="h",
        title="Top 10 Subject Areas",
        labels={"paper_count": "Number of Papers", "subject_name": "Subject Area"},
    )
    if not df_subjects.empty
    else go.Figure()
)

fig_keywords = (
    px.bar(
        df_keywords,
        x="paper_count",
        y="keyword",
        orientation="h",
        title="Top 15 Keywords",
        labels={"paper_count": "Number of Papers", "keyword": "Keyword"},
    )
    if not df_keywords.empty
    else go.Figure()
)

fig_countries = (
    px.bar(
        df_countries,
        x="paper_count",
        y="country",
        orientation="h",
        title="Top 15 Countries by Paper Output",
        labels={"paper_count": "Number of Papers", "country": "Country"},
    )
    if not df_countries.empty
    else go.Figure()
)

# Common figure styling
for fig in [fig_publications, fig_subjects, fig_keywords, fig_countries]:
    fig.update_layout(
        template="plotly_white",
        height=400,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

# Sort y-axis for horizontal bar charts
if not df_subjects.empty:
    fig_subjects.update_yaxes(categoryorder="total ascending")
if not df_keywords.empty:
    fig_keywords.update_yaxes(categoryorder="total ascending")
if not df_countries.empty:
    fig_countries.update_yaxes(categoryorder="total ascending")


def stat_card(
    title: str, value: str | int, subtitle: str | None = None, color: str = "primary"
):
    """Helper to create a Bootstrap card for hero stats."""
    return dbc.Card(
        dbc.CardBody(
            [
                html.Small(title, className="text-muted text-uppercase fw-semibold"),
                html.H3(f"{value}", className=f"mt-2 mb-1 text-{color}"),
                html.Small(subtitle, className="text-muted") if subtitle else None,
            ]
        ),
        className="shadow-sm h-100",
    )


# Layout
layout = dbc.Container(
    [
        # Header
        dbc.Row(
            dbc.Col(
                [
                    html.H1(
                        "Research Database Dashboard",
                        className="display-5 fw-bold text-center text-dark",
                    ),
                    html.P(
                        "High-level overview of publications, impact, and collaboration patterns.",
                        className="text-center text-muted mb-4",
                    ),
                    html.Hr(),
                ],
                width=12,
            ),
            className="mt-3 mb-4",
        ),
        # Overview / Hero cards
        dbc.Row(
            [
                dbc.Col(
                    stat_card(
                        "Total Papers",
                        f"{hero_stats.get('total_papers', 0):,}",
                        "All indexed publications in the database",
                        color="primary",
                    ),
                    md=3,
                    sm=6,
                    className="mb-3",
                ),
                dbc.Col(
                    stat_card(
                        "Unique Authors",
                        f"{hero_stats.get('total_authors', 0):,}",
                        "Distinct author profiles",
                        color="danger",
                    ),
                    md=3,
                    sm=6,
                    className="mb-3",
                ),
                dbc.Col(
                    stat_card(
                        "Institutions",
                        f"{hero_stats.get('total_affiliations', 0):,}",
                        "Affiliated organizations",
                        color="purple",
                    ),
                    md=3,
                    sm=6,
                    className="mb-3",
                ),
                dbc.Col(
                    stat_card(
                        "Total Citations",
                        f"{hero_stats.get('total_citations', 0):,}",
                        f"Year range: {hero_stats.get('earliest_year', 'N/A')} – {hero_stats.get('latest_year', 'N/A')}",
                        color="warning",
                    ),
                    md=3,
                    sm=6,
                    className="mb-3",
                ),
            ],
            className="g-3 mb-5",
        ),
        # Publication Trends
        dbc.Row(
            dbc.Col(
                dbc.Card(
                    [
                        dbc.CardHeader(
                            html.H5("Publication Trends", className="mb-0 fw-semibold")
                        ),
                        dbc.CardBody(
                            dcc.Graph(
                                id="publications-over-time",
                                figure=fig_publications,
                                config={"displayModeBar": False},
                            )
                        ),
                    ],
                    className="shadow-sm",
                ),
                width=12,
            ),
            className="mb-5",
        ),
        # Research Impact: Top cited papers & authors
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5(
                                    "Top 10 Most Cited Papers",
                                    className="mb-0 fw-semibold",
                                )
                            ),
                            dbc.CardBody(
                                (
                                    dag.AgGrid(
                                        rowData=(
                                            df_top_cited.to_dict("records")
                                            if not df_top_cited.empty
                                            else []
                                        ),
                                        columnDefs=[
                                            {
                                                "field": "title_display",
                                                "headerName": "Title",
                                                "flex": 3,
                                                "wrapText": True,
                                                "autoHeight": True,
                                                "cellRenderer": "markdown",
                                            },
                                            {
                                                "field": "authors",
                                                "headerName": "Authors",
                                                "flex": 2,
                                            },
                                            {
                                                "field": "publication_year",
                                                "headerName": "Year",
                                                "width": 90,
                                            },
                                            {
                                                "field": "cited_by_count",
                                                "headerName": "Citations",
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
                                        dangerously_allow_code=True,
                                    )
                                    if not df_top_cited.empty
                                    else html.P(
                                        "No data available", className="text-muted"
                                    )
                                ),
                            ),
                        ],
                        className="shadow-sm h-100",
                    ),
                    md=7,
                    className="mb-4",
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5(
                                    "Top 10 Most Prolific Authors in Chulalongkorn University",
                                    className="mb-0 fw-semibold",
                                )
                            ),
                            dbc.CardBody(
                                (
                                    dag.AgGrid(
                                        rowData=(
                                            df_top_authors.to_dict("records")
                                            if not df_top_authors.empty
                                            else []
                                        ),
                                        columnDefs=[
                                            {
                                                "field": "indexed_name",
                                                "headerName": "Author",
                                                "flex": 2,
                                            },
                                            {
                                                "field": "paper_count",
                                                "headerName": "Papers",
                                                "width": 110,
                                                "type": "numericColumn",
                                                "cellStyle": {"fontWeight": "bold"},
                                            },
                                            {
                                                "field": "total_citations",
                                                "headerName": "Total Citations",
                                                "width": 140,
                                                "type": "numericColumn",
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
                                    )
                                    if not df_top_authors.empty
                                    else html.P(
                                        "No data available", className="text-muted"
                                    )
                                ),
                            ),
                        ],
                        className="shadow-sm h-100",
                    ),
                    md=5,
                    className="mb-4",
                ),
            ],
            className="g-4 mb-5",
        ),
        # Research Landscape: Subject areas & keywords
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5(
                                    "Top Subject Areas", className="mb-0 fw-semibold"
                                )
                            ),
                            dbc.CardBody(
                                dcc.Graph(
                                    id="subject-areas",
                                    figure=fig_subjects,
                                    config={"displayModeBar": False},
                                )
                            ),
                        ],
                        className="shadow-sm h-100",
                    ),
                    md=6,
                    className="mb-4",
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5("Top Keywords", className="mb-0 fw-semibold")
                            ),
                            dbc.CardBody(
                                dcc.Graph(
                                    id="top-keywords",
                                    figure=fig_keywords,
                                    config={"displayModeBar": False},
                                )
                            ),
                        ],
                        className="shadow-sm h-100",
                    ),
                    md=6,
                    className="mb-4",
                ),
            ],
            className="g-4 mb-5",
        ),
        # Collaboration & Geography
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5(
                                    "Top Countries by Paper Output",
                                    className="mb-0 fw-semibold",
                                )
                            ),
                            dbc.CardBody(
                                dcc.Graph(
                                    id="papers-by-country",
                                    figure=fig_countries,
                                    config={"displayModeBar": False},
                                )
                            ),
                        ],
                        className="shadow-sm h-100",
                    ),
                    md=6,
                    className="mb-4",
                ),
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5(
                                    "Top Institutions by Paper Output",
                                    className="mb-0 fw-semibold",
                                )
                            ),
                            dbc.CardBody(
                                (
                                    dag.AgGrid(
                                        rowData=(
                                            df_institutions.to_dict("records")
                                            if not df_institutions.empty
                                            else []
                                        ),
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
                                    )
                                    if not df_institutions.empty
                                    else html.P(
                                        "No data available", className="text-muted"
                                    )
                                ),
                            ),
                        ],
                        className="shadow-sm h-100",
                    ),
                    md=6,
                    className="mb-4",
                ),
            ],
            className="g-4 mb-4",
        ),
    ],
    fluid=True,
    className="py-4 px-3 px-md-4",
)
