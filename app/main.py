from dash import Dash, html, dcc
import dash_ag_grid as dag
import dash
import dash_bootstrap_components as dbc



import pandas as pd
import psycopg2
import dotenv

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Requires Dash 2.17.0 or later
TEMP_PROJECT_NAME = "Research Insights (TEMP)"

def make_navbar():
    # Create nav links from the registered pages
    nav_links = [
        dbc.NavItem(
            dbc.NavLink(
                page["name"],
                href=page["relative_path"],
                active="exact",
            )
        )
        for page in dash.page_registry.values() if len(page["relative_path"].split("/")) <= 2 # Make sure only surface level are rendered
    ]

    return dbc.Navbar(
        dbc.Container(
            [
                # Left: project name / brand
                dbc.NavbarBrand(
                    TEMP_PROJECT_NAME,
                    href="/",
                    className="fw-bold",
                ),

                # Right: page links
                dbc.Nav(
                    nav_links,
                    className="ms-auto",
                    navbar=True,
                ),
            ],
            fluid=True,
        ),
        color="dark",
        dark=True,
        sticky="top",
        className="mb-4 shadow-sm",
    )


app.layout = html.Div(
    [
        make_navbar(),
        dbc.Container(
            dash.page_container,
            fluid=True,
            className="pt-3",
        ),
    ]
)



if __name__ == '__main__':
    app.run(debug=True)