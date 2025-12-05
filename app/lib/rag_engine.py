"""RAG Engine for semantic search and question answering."""

import json
from typing import List, Optional, Dict
from sqlalchemy import Engine, text
import ollama
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import HumanMessage, AIMessage


class RAGEngine:
    """RAG engine for semantic search and question answering using embeddings."""
    
    def __init__(
        self,
        engine: Engine,
        embedding_model: str = "nomic-embed-text",
        llm_model: str = "qwen2.5:7b",
        top_k: int = 5
    ):
        """Initialize RAG engine.
        
        Args:
            engine: SQLAlchemy engine for database connection
            embedding_model: Ollama embedding model name
            llm_model: Ollama LLM model name
            top_k: Number of documents to retrieve
        """
        self.engine = engine
        self.embedding_model = embedding_model
        self.llm_model = llm_model
        self.top_k = top_k
        
        # Initialize LLM
        self.llm = ChatOllama(
            model=llm_model,
            temperature=0.7,
        )
        
        # Create prompt template
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful research assistant with expertise in academic papers. 
Your task is to answer questions about research papers based on the provided context.

Guidelines:
- Answer based ONLY on the provided context papers
- Cite specific papers when making claims
- If the context doesn't contain enough information, say so
- Be precise and academic in your language
- Highlight key findings, methodologies, and implications
- When multiple papers discuss similar topics, synthesize the information

Context Papers:
{context}

Previous conversation:"""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{question}")
        ])
        
        self.chain = self.prompt | self.llm
    
    def embed_query(self, query: str) -> List[float]:
        """Generate embedding for a query string.
        
        Args:
            query: Query text to embed
            
        Returns:
            Embedding vector
        """
        response = ollama.embed(
            model=self.embedding_model,
            input=query
        )
        return response["embeddings"][0]
    
    def semantic_search(
        self,
        query: str,
        top_k: Optional[int] = None,
        context_paper_ids: Optional[List[int]] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None
    ) -> List[Dict]:
        """Perform semantic search using vector similarity.
        
        Args:
            query: Search query
            top_k: Number of results to return (default: self.top_k)
            context_paper_ids: Optional list of paper IDs to restrict search to
            min_year: Minimum publication year
            max_year: Maximum publication year
            
        Returns:
            List of paper dictionaries with similarity scores
        """
        if top_k is None:
            top_k = self.top_k
        
        # Generate query embedding
        query_embedding = self.embed_query(query)
        
        # Build WHERE clause
        where_conditions = []
        params = {"query_embedding": "[" + ",".join(str(x) for x in query_embedding) + "]", "top_k": top_k}
        
        if context_paper_ids:
            placeholders = ",".join([f":paper_id_{i}" for i in range(len(context_paper_ids))])
            where_conditions.append(f"p.paper_id IN ({placeholders})")
            for i, pid in enumerate(context_paper_ids):
                params[f"paper_id_{i}"] = pid
        
        if min_year:
            where_conditions.append("p.publication_year >= :min_year")
            params["min_year"] = min_year
        
        if max_year:
            where_conditions.append("p.publication_year <= :max_year")
            params["max_year"] = max_year
        
        where_clause = " AND " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query with vector similarity
        query_sql = text(f"""
            SELECT 
                p.paper_id,
                p.title,
                p.abstract,
                p.publication_year,
                p.cited_by_count,
                s.source_name,
                STRING_AGG(DISTINCT a.indexed_name, ', ') as authors,
                STRING_AGG(DISTINCT k.keyword, ', ') as keywords,
                1 - (pe.embedding <=> vector(:query_embedding)) as similarity
            FROM papers p
            JOIN paper_embeddings pe ON p.paper_id = pe.paper_id
            LEFT JOIN sources s ON p.source_id = s.source_id
            LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
            LEFT JOIN authors a ON pa.author_id = a.author_id
            LEFT JOIN paper_keywords pk ON p.paper_id = pk.paper_id
            LEFT JOIN keywords k ON pk.keyword_id = k.keyword_id
            WHERE pe.model = 'nomic-embed-text' 
                AND pe.source = 'combined'
                {where_clause}
            GROUP BY p.paper_id, pe.embedding, s.source_name
            ORDER BY pe.embedding <=> vector(:query_embedding)
            LIMIT :top_k
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query_sql, params)
            papers = []
            for row in result:
                papers.append({
                    "paper_id": row[0],
                    "title": row[1],
                    "abstract": row[2] or "",
                    "publication_year": row[3],
                    "cited_by_count": row[4],
                    "source_name": row[5],
                    "authors": row[6] or "",
                    "keywords": row[7] or "",
                    "similarity": float(row[8])
                })
        
        return papers
    
    def get_papers_by_ids(self, paper_ids: List[int]) -> List[Dict]:
        """Fetch full paper details by IDs.
        
        Args:
            paper_ids: List of paper IDs
            
        Returns:
            List of paper dictionaries
        """
        if not paper_ids:
            return []
        
        placeholders = ",".join([f":paper_id_{i}" for i in range(len(paper_ids))])
        params = {f"paper_id_{i}": pid for i, pid in enumerate(paper_ids)}
        
        query = text(f"""
            SELECT 
                p.paper_id,
                p.title,
                p.abstract,
                p.publication_year,
                p.cited_by_count,
                s.source_name,
                STRING_AGG(DISTINCT a.indexed_name, ', ') as authors,
                STRING_AGG(DISTINCT k.keyword, ', ') as keywords
            FROM papers p
            LEFT JOIN sources s ON p.source_id = s.source_id
            LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
            LEFT JOIN authors a ON pa.author_id = a.author_id
            LEFT JOIN paper_keywords pk ON p.paper_id = pk.paper_id
            LEFT JOIN keywords k ON pk.keyword_id = k.keyword_id
            WHERE p.paper_id IN ({placeholders})
            GROUP BY p.paper_id, s.source_name
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, params)
            papers = []
            for row in result:
                papers.append({
                    "paper_id": row[0],
                    "title": row[1],
                    "abstract": row[2] or "",
                    "publication_year": row[3],
                    "cited_by_count": row[4],
                    "source_name": row[5],
                    "authors": row[6] or "",
                    "keywords": row[7] or ""
                })
        
        return papers
    
    def format_context(self, papers: List[Dict]) -> str:
        """Format papers as context for LLM.
        
        Args:
            papers: List of paper dictionaries
            
        Returns:
            Formatted context string
        """
        if not papers:
            return "No relevant papers found."
        
        context_parts = []
        for i, paper in enumerate(papers, 1):
            context_parts.append(
                f"[Paper {i}]\n"
                f"Title: {paper['title']}\n"
                f"Authors: {paper['authors']}\n"
                f"Year: {paper['publication_year']}\n"
                f"Journal: {paper.get('source_name', 'N/A')}\n"
                f"Citations: {paper['cited_by_count']}\n"
                f"Keywords: {paper.get('keywords', 'N/A')}\n"
                f"Abstract: {paper['abstract'][:1000]}...\n"
            )
        
        return "\n\n".join(context_parts)
    
    def answer_question(
        self,
        question: str,
        context_paper_ids: Optional[List[int]] = None,
        chat_history: Optional[List[Dict]] = None,
        top_k: Optional[int] = None
    ) -> str:
        """Answer a question using RAG.
        
        Args:
            question: User's question
            context_paper_ids: Optional specific papers to use as context
            chat_history: Optional previous conversation messages
            top_k: Number of papers to retrieve (if not using context_paper_ids)
            
        Returns:
            Answer string with [SOURCES] JSON appended
        """
        # Get relevant papers
        if context_paper_ids:
            # Use specific papers + semantic search within them
            context_papers = self.get_papers_by_ids(context_paper_ids)
            
            # Also do semantic search to rank and add more relevant papers
            search_results = self.semantic_search(
                query=question,
                top_k=top_k or self.top_k,
                context_paper_ids=context_paper_ids if len(context_paper_ids) <= 20 else None
            )
            
            # Merge results, prioritizing context papers
            paper_map = {p["paper_id"]: p for p in context_papers}
            for paper in search_results:
                if paper["paper_id"] not in paper_map:
                    paper_map[paper["paper_id"]] = paper
            
            relevant_papers = list(paper_map.values())[:self.top_k]
        else:
            # Regular semantic search
            relevant_papers = self.semantic_search(query=question, top_k=top_k)
        
        # Format context
        context = self.format_context(relevant_papers)
        
        # Convert chat history to LangChain messages
        history_messages = []
        if chat_history:
            for msg in chat_history:
                if msg["role"] == "user":
                    history_messages.append(HumanMessage(content=msg["content"]))
                elif msg["role"] == "assistant":
                    # Strip sources from history
                    content = msg["content"].split("[SOURCES]")[0].strip()
                    history_messages.append(AIMessage(content=content))
        
        # Generate answer
        response = self.chain.invoke({
            "context": context,
            "chat_history": history_messages,
            "question": question
        })
        
        # Format sources
        sources = [
            {
                "paper_id": p["paper_id"],
                "title": p["title"],
                "similarity": p.get("similarity", 1.0),
                "cited_by_count": p["cited_by_count"]
            }
            for p in relevant_papers
        ]
        
        # Append sources as JSON
        answer = response.content + "\n\n[SOURCES]\n" + json.dumps(sources)
        
        return answer
    
    def suggest_papers(
        self,
        query: str,
        limit: int = 10,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None
    ) -> List[Dict]:
        """Suggest papers based on a query.
        
        Args:
            query: Search query
            limit: Maximum number of suggestions
            min_year: Minimum publication year
            max_year: Maximum publication year
            
        Returns:
            List of suggested papers
        """
        return self.semantic_search(
            query=query,
            top_k=limit,
            min_year=min_year,
            max_year=max_year
        )