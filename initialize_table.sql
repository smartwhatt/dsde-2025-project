-- =============================================
-- CORE TABLES
-- =============================================

-- Main papers/documents table
CREATE TABLE papers (
    paper_id SERIAL PRIMARY KEY,
    scopus_id VARCHAR(50) UNIQUE NOT NULL,
    eid VARCHAR(100) UNIQUE,
    doi VARCHAR(255),
    title TEXT NOT NULL,
    abstract TEXT,
    publication_date DATE,
    publication_year INTEGER,
    
    -- Journal/Source information
    source_id INTEGER,
    source_type VARCHAR(50), -- 'j' for journal, 'p' for conference, etc.
    volume VARCHAR(50),
    issue VARCHAR(50),
    page_range VARCHAR(50),
    start_page VARCHAR(20),
    end_page VARCHAR(20),
    
    -- Metrics
    cited_by_count INTEGER DEFAULT 0,
    
    -- Access & Classification
    open_access BOOLEAN,
    document_type VARCHAR(100), -- 'ar' for article, 'cp' for conference paper, etc.
    subtype_description VARCHAR(255),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_year CHECK (publication_year >= 1900 AND publication_year <= 2100)
);

-- Source/Journal table
CREATE TABLE sources (
    source_id SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_abbrev VARCHAR(255),
    scopus_source_id VARCHAR(50) UNIQUE,
    issn_print VARCHAR(20),
    issn_electronic VARCHAR(20),
    publisher VARCHAR(500),
    source_type VARCHAR(50), -- 'journal', 'conference', 'book series'
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(source_name, issn_print)
);

-- Authors table
CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    auid VARCHAR(50) UNIQUE NOT NULL, -- Scopus author ID
    surname VARCHAR(255),
    given_name VARCHAR(255),
    initials VARCHAR(20),
    indexed_name VARCHAR(500),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Affiliations/Institutions table
CREATE TABLE affiliations (
    affiliation_id SERIAL PRIMARY KEY,
    scopus_affiliation_id VARCHAR(50) UNIQUE NOT NULL,
    affiliation_name TEXT NOT NULL,
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    postal_code VARCHAR(20),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- JUNCTION/RELATIONSHIP TABLES
-- =============================================

-- Paper-Author relationship (authorship)
CREATE TABLE paper_authors (
    paper_author_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    author_id INTEGER NOT NULL REFERENCES authors(author_id) ON DELETE CASCADE,
    author_sequence INTEGER NOT NULL, -- Order of authors (@seq)
    is_corresponding BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(paper_id, author_id),
    CONSTRAINT chk_sequence CHECK (author_sequence > 0)
);

-- Author-Affiliation relationship (for this specific paper)
CREATE TABLE paper_author_affiliations (
    paa_id SERIAL PRIMARY KEY,
    paper_author_id INTEGER NOT NULL REFERENCES paper_authors(paper_author_id) ON DELETE CASCADE,
    affiliation_id INTEGER NOT NULL REFERENCES affiliations(affiliation_id) ON DELETE CASCADE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(paper_author_id, affiliation_id)
);

-- =============================================
-- KEYWORDS & CLASSIFICATION
-- =============================================

-- Keywords table
CREATE TABLE keywords (
    keyword_id SERIAL PRIMARY KEY,
    keyword TEXT UNIQUE NOT NULL,
    keyword_type VARCHAR(50), -- 'author', 'indexed'
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Paper-Keyword relationship
CREATE TABLE paper_keywords (
    paper_keyword_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    keyword_id INTEGER NOT NULL REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    keyword_type VARCHAR(50), -- 'author', 'indexed'
    
    UNIQUE(paper_id, keyword_id)
);

-- Subject areas table
CREATE TABLE subject_areas (
    subject_area_id SERIAL PRIMARY KEY,
    subject_code VARCHAR(20) UNIQUE NOT NULL,
    subject_name VARCHAR(500) NOT NULL,
    subject_abbrev VARCHAR(20)
);

-- Paper-Subject Area relationship
CREATE TABLE paper_subject_areas (
    paper_subject_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    subject_area_id INTEGER NOT NULL REFERENCES subject_areas(subject_area_id) ON DELETE CASCADE,
    
    UNIQUE(paper_id, subject_area_id)
);

-- =============================================
-- FUNDING
-- =============================================

-- Funding agencies table
CREATE TABLE funding_agencies (
    agency_id SERIAL PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_acronym VARCHAR(50),
    agency_country VARCHAR(255),
    scopus_agency_id VARCHAR(255) UNIQUE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(agency_name, agency_country)
);

-- Paper funding relationship
CREATE TABLE paper_funding (
    funding_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    agency_id INTEGER NOT NULL REFERENCES funding_agencies(agency_id) ON DELETE CASCADE,
    grant_id VARCHAR(255), -- Can be NULL for unspecified grants
    funding_text TEXT, -- Original funding acknowledgment text
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- REFERENCES/CITATIONS
-- =============================================

-- References table (papers cited by our papers)
CREATE TABLE reference_papers (
    reference_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    reference_sequence INTEGER NOT NULL,
    reference_fulltext TEXT,
    
    -- Structured citation data (if available)
    cited_title TEXT,
    cited_source TEXT,
    cited_year INTEGER,
    cited_volume VARCHAR(50),
    cited_pages VARCHAR(50),
    cited_doi VARCHAR(255),
    cited_scopus_id VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(paper_id, reference_sequence)
);

-- =============================================
-- INDICES FOR PERFORMANCE
-- =============================================

-- Papers indices
CREATE INDEX idx_papers_scopus_id ON papers(scopus_id);
CREATE INDEX idx_papers_doi ON papers(doi);
CREATE INDEX idx_papers_publication_year ON papers(publication_year);
CREATE INDEX idx_papers_source_id ON papers(source_id);
CREATE INDEX idx_papers_cited_by_count ON papers(cited_by_count);

-- Authors indices
CREATE INDEX idx_authors_auid ON authors(auid);
CREATE INDEX idx_authors_surname ON authors(surname);
CREATE INDEX idx_authors_indexed_name ON authors(indexed_name);

-- Affiliations indices
CREATE INDEX idx_affiliations_scopus_id ON affiliations(scopus_affiliation_id);
CREATE INDEX idx_affiliations_country ON affiliations(country);
CREATE INDEX idx_affiliations_name ON affiliations(affiliation_name);

-- Paper-Author relationship indices
CREATE INDEX idx_paper_authors_paper_id ON paper_authors(paper_id);
CREATE INDEX idx_paper_authors_author_id ON paper_authors(author_id);
CREATE INDEX idx_paper_authors_sequence ON paper_authors(author_sequence);

-- Paper-Author-Affiliation indices
CREATE INDEX idx_paa_paper_author_id ON paper_author_affiliations(paper_author_id);
CREATE INDEX idx_paa_affiliation_id ON paper_author_affiliations(affiliation_id);

-- Keywords indices
CREATE INDEX idx_keywords_keyword ON keywords(keyword);
CREATE INDEX idx_paper_keywords_paper_id ON paper_keywords(paper_id);
CREATE INDEX idx_paper_keywords_keyword_id ON paper_keywords(keyword_id);

-- Subject areas indices
CREATE INDEX idx_paper_subject_areas_paper_id ON paper_subject_areas(paper_id);
CREATE INDEX idx_paper_subject_areas_subject_id ON paper_subject_areas(subject_area_id);

-- Funding indices
CREATE INDEX idx_paper_funding_paper_id ON paper_funding(paper_id);
CREATE INDEX idx_paper_funding_agency_id ON paper_funding(agency_id);
CREATE INDEX idx_funding_agencies_name ON funding_agencies(agency_name);

-- References indices
CREATE INDEX idx_references_paper_id ON reference_papers(paper_id);
CREATE INDEX idx_references_cited_scopus_id ON reference_papers(cited_scopus_id);

-- Source indices
CREATE INDEX idx_sources_name ON sources(source_name);
CREATE INDEX idx_sources_scopus_id ON sources(scopus_source_id);

-- =============================================
-- FOREIGN KEY CONSTRAINTS
-- =============================================

ALTER TABLE papers 
    ADD CONSTRAINT fk_papers_source 
    FOREIGN KEY (source_id) 
    REFERENCES sources(source_id);

-- =============================================
-- USEFUL VIEWS
-- =============================================

-- Complete paper view with all details
CREATE VIEW vw_papers_complete AS
SELECT 
    p.paper_id,
    p.scopus_id,
    p.doi,
    p.title,
    p.abstract,
    p.publication_year,
    p.cited_by_count,
    s.source_name,
    s.publisher,
    COUNT(DISTINCT pa.author_id) as author_count,
    COUNT(DISTINCT pk.keyword_id) as keyword_count,
    COUNT(DISTINCT pf.agency_id) as funding_agency_count
FROM papers p
LEFT JOIN sources s ON p.source_id = s.source_id
LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
LEFT JOIN paper_keywords pk ON p.paper_id = pk.paper_id
LEFT JOIN paper_funding pf ON p.paper_id = pf.paper_id
GROUP BY p.paper_id, s.source_id;

-- Author collaboration view
CREATE VIEW vw_author_collaborations AS
SELECT 
    pa1.author_id as author_1_id,
    pa2.author_id as author_2_id,
    COUNT(*) as collaboration_count
FROM paper_authors pa1
JOIN paper_authors pa2 ON pa1.paper_id = pa2.paper_id
WHERE pa1.author_id < pa2.author_id
GROUP BY pa1.author_id, pa2.author_id;

-- Author productivity view
CREATE VIEW vw_author_productivity AS
SELECT 
    a.author_id,
    a.surname,
    a.given_name,
    COUNT(DISTINCT pa.paper_id) as paper_count,
    SUM(p.cited_by_count) as total_citations,
    AVG(p.cited_by_count) as avg_citations_per_paper,
    MIN(p.publication_year) as first_publication_year,
    MAX(p.publication_year) as last_publication_year
FROM authors a
LEFT JOIN paper_authors pa ON a.author_id = pa.author_id
LEFT JOIN papers p ON pa.paper_id = p.paper_id
GROUP BY a.author_id;

-- Institution productivity view
CREATE VIEW vw_affiliation_productivity AS
SELECT 
    af.affiliation_id,
    af.affiliation_name,
    af.country,
    COUNT(DISTINCT pa.paper_id) as paper_count,
    COUNT(DISTINCT pa.author_id) as unique_authors
FROM affiliations af
JOIN paper_author_affiliations paa ON af.affiliation_id = paa.affiliation_id
JOIN paper_authors pa ON paa.paper_author_id = pa.paper_author_id
GROUP BY af.affiliation_id;

-- Paper Embedding Table
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE paper_embeddings (
    embedding_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,

    -- which model created this embedding
    model VARCHAR(100) NOT NULL,

    -- which field it embeds: 'title', 'abstract', 'combined'
    source VARCHAR(50) DEFAULT 'combined',

    embedding vector(768) NOT NULL,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_paper_embedding_unique
ON paper_embeddings (paper_id, model, source);



-- =============================================
-- TRIGGER FOR UPDATED_AT
-- =============================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_papers_updated_at
    BEFORE UPDATE ON papers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =============================================
-- COMMENTS
-- =============================================

COMMENT ON TABLE papers IS 'Main table storing paper/document metadata';
COMMENT ON TABLE authors IS 'Author master table with Scopus author IDs';
COMMENT ON TABLE affiliations IS 'Institution/affiliation master table';
COMMENT ON TABLE paper_authors IS 'Junction table linking papers to authors with sequence';
COMMENT ON TABLE paper_author_affiliations IS 'Links authors to their affiliations for specific papers';
COMMENT ON TABLE funding_agencies IS 'Master table of funding agencies';
COMMENT ON TABLE paper_funding IS 'Funding information for each paper';
COMMENT ON TABLE keywords IS 'Master keyword table';
COMMENT ON TABLE paper_keywords IS 'Links papers to keywords';
COMMENT ON TABLE subject_areas IS 'Scopus subject area classifications';
COMMENT ON TABLE reference_papers IS 'References cited by papers in our dataset';
