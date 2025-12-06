from dash import Dash, html, dcc, callback, Input, Output, State, ALL, ctx, MATCH
import dash_ag_grid as dag
import dash
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import text
import json
import threading
import queue

from database import engine
from lib.rag_engine import RAGEngine

dash.register_page(__name__)

# Initialize RAG engine
rag_engine = RAGEngine(engine)

# Global streaming state
streaming_queue = queue.Queue()
streaming_lock = threading.Lock()


def create_message_bubble(message, is_user=True, is_streaming=False):
    """Create a chat message bubble."""
    if is_user:
        return dbc.Card(
            dbc.CardBody([html.Div(message, className="mb-0")]),
            className="mb-3 ms-auto",
            style={"maxWidth": "75%", "backgroundColor": "#0d6efd", "color": "white"},
        )
    else:
        # Parse assistant message for citations
        parts = message.split("[SOURCES]")
        message_text = parts[0].strip()
        sources = []

        if len(parts) > 1:
            try:
                sources = json.loads(parts[1].strip())
            except:
                pass

        # Add cursor for streaming effect
        display_text = message_text
        if is_streaming:
            display_text += "▊"  # Cursor

        return dbc.Card(
            dbc.CardBody(
                [
                    dcc.Markdown(display_text, className="mb-2"),
                    (
                        html.Div(
                            [
                                html.Hr(className="my-2"),
                                html.H6("📚 Sources:", className="mb-2 text-muted"),
                                html.Div(
                                    [
                                        dbc.Card(
                                            [
                                                dbc.CardBody(
                                                    [
                                                        html.A(
                                                            [
                                                                html.Strong(
                                                                    src.get(
                                                                        "title",
                                                                        "Untitled",
                                                                    )[:100]
                                                                ),
                                                                html.Br(),
                                                                html.Small(
                                                                    f"Relevance: {src.get('similarity', 0):.2%} | Citations: {src.get('cited_by_count', 0)}",
                                                                    className="text-muted",
                                                                ),
                                                            ],
                                                            href=f"/papers/{src['paper_id']}",
                                                            className="text-decoration-none",
                                                            target="_blank",
                                                        )
                                                    ],
                                                    className="py-2",
                                                )
                                            ],
                                            className="mb-2",
                                        )
                                        for src in sources
                                    ]
                                ),
                            ],
                            className="mt-2",
                        )
                        if sources and not is_streaming
                        else None
                    ),
                ]
            ),
            className="mb-3 me-auto",
            style={"maxWidth": "75%", "backgroundColor": "#f8f9fa"},
        )


def stream_llm_response(question, context_ids, history, session_id):
    """Stream LLM response in a background thread."""
    try:
        # Use the RAG engine's LLM with streaming
        from langchain_core.messages import HumanMessage, AIMessage

        # Get relevant papers
        if context_ids:
            context_papers = rag_engine.get_papers_by_ids(context_ids)
            search_results = rag_engine.semantic_search(
                query=question,
                top_k=rag_engine.top_k,
                context_paper_ids=context_ids if len(context_ids) <= 20 else None,
            )
            paper_map = {p["paper_id"]: p for p in context_papers}
            for paper in search_results:
                if paper["paper_id"] not in paper_map:
                    paper_map[paper["paper_id"]] = paper
            relevant_papers = list(paper_map.values())[: rag_engine.top_k]
        else:
            relevant_papers = rag_engine.semantic_search(query=question)

        # Format context
        context = rag_engine.format_context(relevant_papers)

        # Convert chat history
        history_messages = []
        if history:
            for msg in history:
                if msg["role"] == "user":
                    history_messages.append(HumanMessage(content=msg["content"]))
                elif msg["role"] == "assistant":
                    content = msg["content"].split("[SOURCES]")[0].strip()
                    history_messages.append(AIMessage(content=content))

        # Stream the response
        full_response = ""
        for chunk in rag_engine.chain.stream(
            {"context": context, "chat_history": history_messages, "question": question}
        ):
            if hasattr(chunk, "content"):
                full_response += chunk.content
                streaming_queue.put(
                    {
                        "session_id": session_id,
                        "type": "chunk",
                        "content": full_response,
                    }
                )

        # Format sources
        sources = [
            {
                "paper_id": p["paper_id"],
                "title": p["title"],
                "similarity": p.get("similarity", 1.0),
                "cited_by_count": p["cited_by_count"],
            }
            for p in relevant_papers
        ]

        # Send final message with sources
        final_answer = full_response + "\n\n[SOURCES]\n" + json.dumps(sources)
        streaming_queue.put(
            {"session_id": session_id, "type": "complete", "content": final_answer}
        )

    except Exception as e:
        streaming_queue.put(
            {"session_id": session_id, "type": "error", "content": f"Error: {str(e)}"}
        )


def get_paper_details(paper_id):
    """Fetch paper details for context."""
    try:
        query = text(
            """
            SELECT 
                p.paper_id,
                p.title,
                p.abstract,
                p.publication_year,
                p.cited_by_count,
                STRING_AGG(DISTINCT k.keyword, ', ') as keywords
            FROM papers p
            LEFT JOIN paper_keywords pk ON p.paper_id = pk.paper_id
            LEFT JOIN keywords k ON pk.keyword_id = k.keyword_id
            WHERE p.paper_id = :paper_id
            GROUP BY p.paper_id
        """
        )

        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={"paper_id": paper_id})
            return df.iloc[0].to_dict() if not df.empty else None
    except Exception as e:
        print(f"Error fetching paper: {e}")
        return None


layout = dbc.Container(
    [
        # Header
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H1(
                            "Research Assistant Chat",
                            className="display-5 fw-bold text-center text-dark mt-4",
                        ),
                        html.P(
                            "Ask questions about research papers and get AI-powered answers with citations",
                            className="text-center text-muted mb-4",
                        ),
                        html.Hr(),
                    ],
                    width=12,
                )
            ]
        ),
        # Context Papers Section
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    [
                                        html.H5(
                                            [
                                                html.I(
                                                    className="bi bi-bookmarks me-2"
                                                ),
                                                "Context Papers",
                                            ],
                                            className="mb-0 fw-semibold d-inline",
                                        ),
                                        dbc.Button(
                                            [
                                                html.I(
                                                    className="bi bi-plus-circle me-1"
                                                ),
                                                "Add Paper",
                                            ],
                                            id="add-paper-btn",
                                            color="primary",
                                            size="sm",
                                            className="float-end",
                                        ),
                                    ]
                                ),
                                dbc.CardBody(
                                    [
                                        html.Div(
                                            id="context-papers-list",
                                            children=[
                                                dbc.Alert(
                                                    "No papers added to context. Add papers to focus the conversation on specific research.",
                                                    color="info",
                                                    className="mb-0",
                                                )
                                            ],
                                        )
                                    ]
                                ),
                            ],
                            className="shadow-sm mb-4",
                        )
                    ],
                    width=12,
                )
            ]
        ),
        # Modal for adding papers
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Search and Add Papers")),
                dbc.ModalBody(
                    [
                        dbc.Input(
                            id="paper-search-modal",
                            placeholder="Search by title, author, or keyword...",
                            type="text",
                            className="mb-3",
                        ),
                        html.Div(
                            id="paper-search-results",
                            style={"maxHeight": "400px", "overflowY": "auto"},
                        ),
                    ]
                ),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-modal-btn", className="ms-auto")
                ),
            ],
            id="add-paper-modal",
            size="lg",
            is_open=False,
        ),
        # Chat Interface
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    [
                                        html.H5(
                                            [
                                                html.I(
                                                    className="bi bi-chat-dots me-2"
                                                ),
                                                "Chat",
                                            ],
                                            className="mb-0 fw-semibold d-inline",
                                        ),
                                        dbc.Button(
                                            [
                                                html.I(className="bi bi-trash me-1"),
                                                "Clear",
                                            ],
                                            id="clear-chat-btn",
                                            color="outline-secondary",
                                            size="sm",
                                            className="float-end",
                                        ),
                                    ]
                                ),
                                dbc.CardBody(
                                    [
                                        html.Div(
                                            id="chat-messages",
                                            style={
                                                "height": "500px",
                                                "overflowY": "auto",
                                                "padding": "1rem",
                                                "backgroundColor": "#ffffff",
                                            },
                                            children=[
                                                html.Div(
                                                    [
                                                        html.I(
                                                            className="bi bi-robot fs-1 text-muted mb-3"
                                                        ),
                                                        html.H5(
                                                            "Welcome to Research Assistant!",
                                                            className="text-muted",
                                                        ),
                                                        html.P(
                                                            "Ask me anything about the research papers in the database.",
                                                            className="text-muted",
                                                        ),
                                                    ],
                                                    className="text-center mt-5",
                                                )
                                            ],
                                        )
                                    ],
                                    className="p-0",
                                ),
                                dbc.CardFooter(
                                    [
                                        dbc.InputGroup(
                                            [
                                                dbc.Input(
                                                    id="chat-input",
                                                    placeholder="Ask a question about research papers...",
                                                    type="text",
                                                    style={
                                                        "borderRadius": "20px 0 0 20px"
                                                    },
                                                ),
                                                dbc.Button(
                                                    [
                                                        html.I(
                                                            className="bi bi-send-fill"
                                                        )
                                                    ],
                                                    id="send-btn",
                                                    color="primary",
                                                    style={
                                                        "borderRadius": "0 20px 20px 0"
                                                    },
                                                ),
                                            ]
                                        )
                                    ]
                                ),
                            ],
                            className="shadow-sm",
                        )
                    ],
                    width=12,
                )
            ]
        ),
        # Hidden stores and interval
        dcc.Store(id="chat-history", data=[]),
        dcc.Store(id="context-paper-ids", data=[]),
        dcc.Store(id="url-params", data={}),
        dcc.Store(
            id="streaming-state",
            data={"is_streaming": False, "session_id": None, "current_response": ""},
        ),
        dcc.Location(id="url", refresh=False),
        dcc.Interval(
            id="stream-interval", interval=100, disabled=True
        ),  # 100ms update rate
    ],
    fluid=True,
    className="py-4 px-3 px-md-4",
)


# Parse URL parameters
@callback(Output("url-params", "data"), Input("url", "search"))
def parse_url(search):
    """Parse URL query parameters."""
    if not search:
        return {}

    params = {}
    if search.startswith("?"):
        search = search[1:]

    for param in search.split("&"):
        if "=" in param:
            key, value = param.split("=", 1)
            params[key] = value

    return params


# Load paper from URL
@callback(
    Output("context-paper-ids", "data", allow_duplicate=True),
    Output("context-papers-list", "children", allow_duplicate=True),
    Input("url-params", "data"),
    State("context-paper-ids", "data"),
    prevent_initial_call=True,
)
def load_paper_from_url(url_params, current_ids):
    """Load paper from URL parameter."""
    if not url_params or "paper_id" not in url_params:
        return current_ids, dash.no_update

    try:
        paper_id = int(url_params["paper_id"])
        if paper_id not in current_ids:
            current_ids.append(paper_id)

            # Generate paper cards
            paper_cards = []
            for pid in current_ids:
                paper = get_paper_details(pid)
                if paper:
                    paper_cards.append(
                        dbc.Card(
                            [
                                dbc.CardBody(
                                    [
                                        html.Div(
                                            [
                                                html.Strong(paper["title"]),
                                                dbc.Button(
                                                    html.I(className="bi bi-x"),
                                                    id={
                                                        "type": "remove-paper",
                                                        "index": pid,
                                                    },
                                                    color="link",
                                                    size="sm",
                                                    className="float-end p-0 text-danger",
                                                ),
                                            ]
                                        ),
                                        html.Small(
                                            f"Year: {paper['publication_year']} | Citations: {paper['cited_by_count']}",
                                            className="text-muted",
                                        ),
                                    ]
                                )
                            ],
                            className="mb-2",
                        )
                    )

            return current_ids, paper_cards
    except:
        pass

    return current_ids, dash.no_update


# Toggle modal
@callback(
    Output("add-paper-modal", "is_open"),
    [Input("add-paper-btn", "n_clicks"), Input("close-modal-btn", "n_clicks")],
    [State("add-paper-modal", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open


# Search papers in modal
@callback(
    Output("paper-search-results", "children"), Input("paper-search-modal", "value")
)
def search_papers_modal(search_query):
    """Search papers for adding to context."""
    if not search_query or len(search_query) < 3:
        return html.P(
            "Enter at least 3 characters to search...", className="text-muted"
        )

    try:
        query = text(
            """
            SELECT p.paper_id, p.title, p.publication_year, p.cited_by_count
            FROM papers p
            WHERE LOWER(p.title) LIKE LOWER(:search)
            ORDER BY p.cited_by_count DESC
            LIMIT 10
        """
        )

        with engine.connect() as connection:
            df = pd.read_sql_query(
                query, connection, params={"search": f"%{search_query}%"}
            )

        if df.empty:
            return html.P("No papers found.", className="text-muted")

        return html.Div(
            [
                dbc.ListGroup(
                    [
                        dbc.ListGroupItem(
                            [
                                html.Div(
                                    [
                                        html.Strong(row["title"]),
                                        dbc.Button(
                                            "Add",
                                            id={
                                                "type": "add-context-paper",
                                                "index": row["paper_id"],
                                            },
                                            color="primary",
                                            size="sm",
                                            className="float-end",
                                        ),
                                    ]
                                ),
                                html.Small(
                                    f"Year: {row['publication_year']} | Citations: {row['cited_by_count']}",
                                    className="text-muted",
                                ),
                            ]
                        )
                        for _, row in df.iterrows()
                    ]
                )
            ]
        )

    except Exception as e:
        return html.P(f"Error: {str(e)}", className="text-danger")


# Add paper to context
@callback(
    Output("context-paper-ids", "data"),
    Output("context-papers-list", "children"),
    Output("add-paper-modal", "is_open", allow_duplicate=True),
    Input({"type": "add-context-paper", "index": ALL}, "n_clicks"),
    State("context-paper-ids", "data"),
    prevent_initial_call=True,
)
def add_paper_to_context(n_clicks, current_ids):
    """Add selected paper to context."""
    if not any(n_clicks):
        return current_ids, dash.no_update, dash.no_update

    # Get the paper_id from the triggered button
    triggered = ctx.triggered_id
    if triggered and isinstance(triggered, dict):
        paper_id = triggered["index"]

        if paper_id not in current_ids:
            current_ids.append(paper_id)

    # Generate paper cards
    paper_cards = []
    for pid in current_ids:
        paper = get_paper_details(pid)
        if paper:
            paper_cards.append(
                dbc.Card(
                    [
                        dbc.CardBody(
                            [
                                html.Div(
                                    [
                                        html.A(
                                            paper["title"],
                                            href=f"/papers/{pid}",
                                            className="text-decoration-none fw-bold",
                                            target="_blank",
                                        ),
                                        dbc.Button(
                                            html.I(className="bi bi-x"),
                                            id={"type": "remove-paper", "index": pid},
                                            color="link",
                                            size="sm",
                                            className="float-end p-0 text-danger",
                                        ),
                                    ]
                                ),
                                html.Small(
                                    f"Year: {paper['publication_year']} | Citations: {paper['cited_by_count']}",
                                    className="text-muted d-block mt-1",
                                ),
                                (
                                    html.Small(
                                        f"Keywords: {paper.get('keywords', 'N/A')[:100]}...",
                                        className="text-muted",
                                    )
                                    if paper.get("keywords")
                                    else None
                                ),
                            ]
                        )
                    ],
                    className="mb-2",
                )
            )

    if not paper_cards:
        paper_cards = [
            dbc.Alert("No papers added to context.", color="info", className="mb-0")
        ]

    return current_ids, paper_cards, False


# Remove paper from context
@callback(
    Output("context-paper-ids", "data", allow_duplicate=True),
    Output("context-papers-list", "children", allow_duplicate=True),
    Input({"type": "remove-paper", "index": ALL}, "n_clicks"),
    State("context-paper-ids", "data"),
    prevent_initial_call=True,
)
def remove_paper_from_context(n_clicks, current_ids):
    """Remove paper from context."""
    if not any(n_clicks):
        return current_ids, dash.no_update

    triggered = ctx.triggered_id
    if triggered and isinstance(triggered, dict):
        paper_id = triggered["index"]
        if paper_id in current_ids:
            current_ids.remove(paper_id)

    # Regenerate cards
    paper_cards = []
    for pid in current_ids:
        paper = get_paper_details(pid)
        if paper:
            paper_cards.append(
                dbc.Card(
                    [
                        dbc.CardBody(
                            [
                                html.Div(
                                    [
                                        html.A(
                                            paper["title"],
                                            href=f"/papers/{pid}",
                                            className="text-decoration-none fw-bold",
                                            target="_blank",
                                        ),
                                        dbc.Button(
                                            html.I(className="bi bi-x"),
                                            id={"type": "remove-paper", "index": pid},
                                            color="link",
                                            size="sm",
                                            className="float-end p-0 text-danger",
                                        ),
                                    ]
                                ),
                                html.Small(
                                    f"Year: {paper['publication_year']} | Citations: {paper['cited_by_count']}",
                                    className="text-muted d-block mt-1",
                                ),
                                (
                                    html.Small(
                                        f"Keywords: {paper.get('keywords', 'N/A')[:100]}...",
                                        className="text-muted",
                                    )
                                    if paper.get("keywords")
                                    else None
                                ),
                            ]
                        )
                    ],
                    className="mb-2",
                )
            )

    if not paper_cards:
        paper_cards = [
            dbc.Alert("No papers added to context.", color="info", className="mb-0")
        ]

    return current_ids, paper_cards


# Start streaming when user sends message
@callback(
    Output("chat-history", "data", allow_duplicate=True),
    Output("chat-input", "value"),
    Output("streaming-state", "data"),
    Output("stream-interval", "disabled"),
    [Input("send-btn", "n_clicks"), Input("chat-input", "n_submit")],
    [
        State("chat-input", "value"),
        State("chat-history", "data"),
        State("context-paper-ids", "data"),
    ],
    prevent_initial_call=True,
)
def start_streaming(n_clicks, n_submit, message, history, context_ids):
    """Start streaming response in background thread."""
    if not message or not message.strip():
        return (
            history,
            "",
            {"is_streaming": False, "session_id": None, "current_response": ""},
            True,
        )

    # Add user message to history
    history.append({"role": "user", "content": message})

    # Generate unique session ID
    import time

    session_id = f"session_{int(time.time() * 1000)}"

    # Start streaming in background thread
    thread = threading.Thread(
        target=stream_llm_response,
        args=(message, context_ids, history[:-1], session_id),
    )
    thread.daemon = True
    thread.start()

    # Enable interval to poll for updates
    return (
        history,
        "",
        {"is_streaming": True, "session_id": session_id, "current_response": ""},
        False,
    )


# Update chat with streaming chunks
@callback(
    Output("chat-messages", "children"),
    Output("chat-history", "data", allow_duplicate=True),
    Output("streaming-state", "data", allow_duplicate=True),
    Output("stream-interval", "disabled", allow_duplicate=True),
    Input("stream-interval", "n_intervals"),
    [State("chat-history", "data"), State("streaming-state", "data")],
    prevent_initial_call=True,
)
def update_streaming(n_intervals, history, streaming_state):
    """Update chat messages with streaming chunks."""
    if not streaming_state["is_streaming"]:
        return dash.no_update, history, streaming_state, True

    # Check queue for new chunks
    chunks_found = False
    while not streaming_queue.empty():
        try:
            data = streaming_queue.get_nowait()
            if data["session_id"] == streaming_state["session_id"]:
                chunks_found = True

                if data["type"] == "chunk":
                    streaming_state["current_response"] = data["content"]

                elif data["type"] == "complete":
                    # Add final message to history
                    history.append({"role": "assistant", "content": data["content"]})
                    streaming_state["is_streaming"] = False
                    streaming_state["current_response"] = ""

                elif data["type"] == "error":
                    history.append({"role": "assistant", "content": data["content"]})
                    streaming_state["is_streaming"] = False
                    streaming_state["current_response"] = ""
        except queue.Empty:
            break

    if not chunks_found and not streaming_state["is_streaming"]:
        return dash.no_update, history, streaming_state, True

    # Render messages
    messages = []
    for msg in history:
        messages.append(
            create_message_bubble(msg["content"], is_user=(msg["role"] == "user"))
        )

    # Add streaming message if active
    if streaming_state["is_streaming"] and streaming_state["current_response"]:
        messages.append(
            create_message_bubble(
                streaming_state["current_response"], is_user=False, is_streaming=True
            )
        )

    # Disable interval if streaming complete
    interval_disabled = not streaming_state["is_streaming"]

    return messages, history, streaming_state, interval_disabled


# Clear chat
@callback(
    Output("chat-messages", "children", allow_duplicate=True),
    Output("chat-history", "data", allow_duplicate=True),
    Input("clear-chat-btn", "n_clicks"),
    prevent_initial_call=True,
)
def clear_chat(n_clicks):
    """Clear chat history."""
    return [
        html.Div(
            [
                html.I(className="bi bi-robot fs-1 text-muted mb-3"),
                html.H5("Welcome to Research Assistant!", className="text-muted"),
                html.P(
                    "Ask me anything about the research papers in the database.",
                    className="text-muted",
                ),
            ],
            className="text-center mt-5",
        )
    ], []
