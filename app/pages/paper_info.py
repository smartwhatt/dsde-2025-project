import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import  text
from app.database import engine 

dash.register_page(__name__, path_template="/papers/<paper_id>")




def get_paper_details(paper_id):
    """Fetch comprehensive paper details."""
    try:
        query = text("""
            SELECT 
                p.paper_id,
                p.title,
                p.abstract,
                p.publication_date,
                p.publication_year,
                p.doi,
                -- Aggregate keywords
                STRING_AGG(DISTINCT k.keyword, ', ' ORDER BY k.keyword) as keywords,
                -- Aggregate affiliations
                STRING_AGG(
                    DISTINCT af.affiliation_name || COALESCE(', ' || af.country, ''), 
                    '; '
                    ORDER BY af.affiliation_name || COALESCE(', ' || af.country, '')
                ) as affiliations
            FROM papers p
            LEFT JOIN paper_keywords pk ON p.paper_id = pk.paper_id
            LEFT JOIN keywords k ON pk.keyword_id = k.keyword_id
            LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
            LEFT JOIN paper_author_affiliations paa ON pa.paper_author_id = paa.paper_author_id
            LEFT JOIN affiliations af ON paa.affiliation_id = af.affiliation_id
            WHERE p.paper_id = :paper_id
            GROUP BY p.paper_id
        """)
        
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'paper_id': paper_id})
        
        return df.iloc[0] if not df.empty else None
    except Exception as e:
        print(f"Error fetching paper details: {e}")
        return None


def get_cited_references(paper_id):
    """Fetch cited papers and check if they exist in our database."""
    try:
        # This query joins reference_papers with papers to find internal links
        query = text("""
            SELECT 
                rp.reference_fulltext,
                rp.cited_title,
                rp.cited_source,
                rp.cited_year,
                rp.cited_doi,
                -- Try to find the cited paper in our own papers table
                p_ref.paper_id as internal_link_id
            FROM reference_papers rp
            LEFT JOIN papers p_ref ON rp.cited_scopus_id = p_ref.scopus_id
            WHERE rp.paper_id = :paper_id
            ORDER BY rp.reference_sequence
        """)
        
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'paper_id': paper_id})
        
        return df
    except Exception as e:
        print(f"Error fetching references: {e}")
        return pd.DataFrame()


def generate_apa_reference_item(row):
    """
    Format a single reference row into APA style.
    - If 'internal_link_id' exists, return a clickable Link.
    - Otherwise, return standard black text.
    """
    # 1. Determine the text to display
    if row['reference_fulltext']:
        display_text = row['reference_fulltext']
    else:
        # Fallback construction if fulltext is missing
        parts = []
        if row['cited_title']:
            parts.append(f"{row['cited_title']}.")
        if row['cited_source']:
            parts.append(f"In *{row['cited_source']}*")
        if row['cited_year']:
            parts.append(f"({int(row['cited_year'])}).")
        display_text = " ".join(parts) or "Unknown Reference"

    # 2. Render as Link (Blue) or Text (Black)
    if pd.notnull(row['internal_link_id']):
        # Reference exists in our DB -> Make it a hyperlink
        return html.Li(
            dcc.Link(
                dcc.Markdown(display_text, className="mb-0"),
                href=f"/papers/{int(row['internal_link_id'])}",
                className="text-primary text-decoration-none"
            ),
            className="mb-2"
        )
    else:
        # Reference NOT in our DB -> Standard black text
        return html.Li(
            dcc.Markdown(display_text, className="mb-0"),
            className="mb-2 text-body" # text-body ensures standard black color
        )


def layout(paper_id=None, **kwargs):
    if not paper_id:
        return dbc.Container([html.H3("No Paper ID", className="text-danger mt-5")])
    
    try:
        paper_id_int = int(paper_id)
    except:
        return dbc.Container([html.H3("Invalid ID", className="text-danger mt-5")])
    
    # Fetch Data
    paper = get_paper_details(paper_id_int)
    if paper is None:
        return dbc.Container([html.H3("Paper not found", className="text-danger mt-5")])
    
    references_df = get_cited_references(paper_id_int)

    # Safe get helper
    def val(key): return paper.get(key) or "N/A"

    return dbc.Container([
        # Back Button
        dbc.Button([html.I(className="bi bi-arrow-left me-2"), "Back"], 
                   href="/papers", color="light", className="mb-4 border"),

        # 1. Title
        html.H2(val('title'), className="fw-bold text-dark mb-4"),

        # 2. Affiliation
        html.H5("Affiliations", className="text-primary fw-bold"),
        html.P(val('affiliations'), className="mb-4"),

        # 3. Keywords
        html.H5("Keywords", className="text-primary fw-bold"),
        html.Div([
            dbc.Badge(k.strip(), color="light", text_color="dark", className="me-1 border") 
            for k in (paper['keywords'].split(',') if paper['keywords'] else [])
        ], className="mb-4"),

        # 4. Published Date
        html.H5("Published Date", className="text-primary fw-bold"),
        html.P(f"{val('publication_date')} (Year: {val('publication_year')})", className="mb-4"),

        # 5. Abstract
        html.H5("Abstract", className="text-primary fw-bold"),
        dbc.Card(
            dbc.CardBody(dcc.Markdown(val('abstract'))),
            className="bg-light border-0 mb-4"
        ),

        # 6. Sources (References) in APA Format
        html.H5("Sources (References)", className="text-primary fw-bold"),
        html.Div([
            html.Ul([
                generate_apa_reference_item(row) 
                for _, row in references_df.iterrows()
            ], className="list-unstyled") if not references_df.empty else html.P("No references indexed.", className="text-muted")
        ])

    ], fluid=True, className="py-5 px-4", style={"maxWidth": "1000px"})