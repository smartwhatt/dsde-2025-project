import psycopg2
import json


class ScopusDBLoader:
    def __init__(self, conn_string=None, conn=None):
        """Create a loader.

        Either provide a psycopg2 connection via `conn` (recommended when the
        caller wants to control transactions), or provide a `conn_string` and
        the loader will create and manage its own connection and transactions.

        Behavior:
        - If `conn` is provided: the loader will NOT commit/rollback; the
          caller must manage transactions and close the connection.
        - If `conn_string` is provided: the loader will create the connection
          and will commit/rollback inside `insert_paper` as before.
        """
        if conn is not None:
            # Use caller-supplied connection; caller controls transactions
            self.conn = conn
            self._owns_connection = False
        elif conn_string is not None:
            # Create and own our connection; manage transactions locally
            self.conn = psycopg2.connect(conn_string)
            self._owns_connection = True
        else:
            raise ValueError("Either conn or conn_string must be provided")

        self.cur = self.conn.cursor()
        # If we own the connection, we manage transactions (commit/rollback).
        # If the connection is provided by caller, the caller manages
        # transactions and may call commit/rollback across multiple inserts.
        self._manage_transaction = self._owns_connection

    def insert_paper(self, json_data):
        """Main function to insert a complete paper record.

        If the loader was created with an external `conn`, this function will
        NOT commit or rollback the transaction — the caller must do that.
        If the loader created its own connection via `conn_string`, this
        method will commit on success and rollback on exception (backwards
        compatible behavior).
        """
        coredata = json_data["coredata"]

        try:
            # 1. Insert/Get Source
            source_id = self._insert_source(json_data)

            # 2. Insert Paper
            paper_id = self._insert_paper_core(coredata, source_id)

            # 3. Insert Authors and relationships
            self._insert_authors(json_data, paper_id)

            # 4. Insert Keywords
            self._insert_keywords(json_data, paper_id)

            # 5. Insert Subject Areas
            self._insert_subject_areas(json_data, paper_id)

            # 6. Insert Funding
            self._insert_funding(json_data, paper_id)

            # 7. Insert References
            self._insert_references(json_data, paper_id)

            if self._manage_transaction:
                self.conn.commit()

            return paper_id

        except Exception as e:
            # If we manage transactions, rollback; otherwise leave it to the
            # caller who provided the connection so they can abort a larger
            # transaction spanning multiple paper uploads.
            if self._manage_transaction:
                try:
                    self.conn.rollback()
                except Exception:
                    # If rollback itself fails, still raise original error
                    pass
            print(f"Error processing paper: {e}")
            raise

    def _insert_paper_core(self, coredata, source_id):
        """Insert main paper record"""
        pub_date = coredata.get("prism:coverDate")
        pub_year = int(pub_date[:4]) if pub_date else None

        sql = """
            INSERT INTO papers (
                scopus_id, eid, doi, title, abstract, publication_date,
                publication_year, source_id, source_type, volume, issue,
                page_range, start_page, end_page, cited_by_count,
                open_access, document_type, subtype_description
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (scopus_id) DO UPDATE SET
                cited_by_count = EXCLUDED.cited_by_count,
                updated_at = CURRENT_TIMESTAMP
            RETURNING paper_id
        """

        values = (
            coredata.get("dc:identifier"),
            coredata.get("eid"),
            coredata.get("prism:doi"),
            coredata.get("dc:title"),
            coredata.get("dc:description"),
            pub_date,
            pub_year,
            source_id,
            coredata.get("prism:aggregationType"),
            coredata.get("prism:volume"),
            coredata.get("prism:issueIdentifier"),
            coredata.get("prism:pageRange"),
            coredata.get("prism:startingPage"),
            coredata.get("prism:endingPage"),
            int(coredata.get("citedby-count", 0)),
            coredata.get("openaccess") == "2",
            coredata.get("subtype"),
            coredata.get("subtypeDescription"),
        )

        self.cur.execute(sql, values)
        return self.cur.fetchone()[0]

    def _insert_source(self, json_data):
        """Insert or get source/journal"""
        coredata = json_data["coredata"]
        source_info = json_data["item"]["bibrecord"]["head"]["source"]
        # Prefer to deduplicate by scopus_source_id when present. Not all DB
        # schemas may have a unique constraint on that column, so avoid using
        # ON CONFLICT on it directly. Instead SELECT first and only INSERT if
        # no existing row is found; this avoids InvalidColumnReference errors.
        scopus_src = source_info.get("@srcid")

        if scopus_src:
            try:
                self.cur.execute(
                    "SELECT source_id FROM sources WHERE scopus_source_id=%s",
                    (scopus_src,),
                )
                row = self.cur.fetchone()
                if row:
                    return row[0]
            except Exception:
                # If select fails for any reason, continue to insert path below
                pass

        sql = """
            INSERT INTO sources (
                source_name, source_abbrev, scopus_source_id,
                issn_print, issn_electronic, publisher, source_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING source_id
        """

        issn_list = source_info.get("issn", [])
        issn_print = None
        issn_electronic = None

        if isinstance(issn_list, list):
            for issn in issn_list:
                if issn.get("@type") == "print":
                    issn_print = issn.get("$")
                elif issn.get("@type") == "electronic":
                    issn_electronic = issn.get("$")

        values = (
            coredata.get("prism:publicationName"),
            source_info.get("sourcetitle-abbrev"),
            source_info.get("@srcid"),
            issn_print,
            issn_electronic,
            coredata.get("dc:publisher"),
            source_info.get("@type"),
        )
        # Try inserting; if another process inserted the same source between
        # our SELECT and INSERT, fall back to selecting the existing row.
        try:
            self.cur.execute(sql, values)
            row = self.cur.fetchone()
            if row and row[0] is not None:
                return row[0]
        except Exception:
            # In case of a race that causes a unique violation or other error,
            # try to select the source again. If that fails, re-raise.
            try:
                if scopus_src:
                    self.cur.execute(
                        "SELECT source_id FROM sources WHERE scopus_source_id=%s",
                        (scopus_src,),
                    )
                    row = self.cur.fetchone()
                    if row:
                        return row[0]
            except Exception:
                pass
            raise

        # If insert returned nothing (shouldn't normally happen), attempt a
        # final lookup by scopus id or name/issn as a fallback.
        if scopus_src:
            self.cur.execute(
                "SELECT source_id FROM sources WHERE scopus_source_id=%s",
                (scopus_src,),
            )
            row = self.cur.fetchone()
            if row:
                return row[0]

        # As a last resort, try to find by name + issn_print
        self.cur.execute(
            "SELECT source_id FROM sources WHERE source_name=%s AND issn_print=%s",
            (coredata.get("prism:publicationName"), issn_print),
        )
        row = self.cur.fetchone()
        return row[0] if row else None

    def _insert_authors(self, json_data, paper_id):
        """Insert authors and their affiliations"""
        authors_data = json_data.get("authors", {}).get("author", [])
        affiliations_data = json_data.get("affiliation", [])

        # First, insert all affiliations
        affiliation_map = {}
        if not isinstance(affiliations_data, list):
            affiliations_data = [affiliations_data]
        for aff in affiliations_data:
            aff_id = self._insert_affiliation(aff)
            affiliation_map[aff.get("@id")] = aff_id

        # Then insert authors
        for author in authors_data:
            author_id = self._insert_author(author)
            paper_author_id = self._link_paper_author(
                paper_id, author_id, int(author.get("@seq", 0))
            )

            # Link author to affiliations for this paper
            author_affs = author.get("affiliation", [])
            if not isinstance(author_affs, list):
                author_affs = [author_affs]

            for aff in author_affs:
                aff_scopus_id = aff.get("@id")
                if aff_scopus_id in affiliation_map:
                    # Only attempt to link if we have a valid paper_author_id
                    if paper_author_id:
                        self._link_paper_author_affiliation(
                            paper_author_id, affiliation_map[aff_scopus_id]
                        )
                    else:
                        # Defensive: try to look up an existing paper_author_id
                        try:
                            self.cur.execute(
                                "SELECT paper_author_id FROM paper_authors WHERE paper_id=%s AND author_id=%s",
                                (paper_id, author_id),
                            )
                            row = self.cur.fetchone()
                            if row and row[0]:
                                self._link_paper_author_affiliation(
                                    row[0], affiliation_map[aff_scopus_id]
                                )
                        except Exception:
                            # If lookup fails, skip linking to avoid inserting NULL
                            pass

    def _insert_author(self, author_data):
        """Insert or get author"""
        sql = """
            INSERT INTO authors (auid, surname, given_name, initials, indexed_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (auid) DO UPDATE SET
                surname = EXCLUDED.surname
            RETURNING author_id
        """

        preferred = author_data.get("preferred-name", {})

        values = (
            author_data.get("@auid"),
            author_data.get("ce:surname"),
            preferred.get("ce:given-name"),
            author_data.get("ce:initials"),
            author_data.get("ce:indexed-name"),
        )

        self.cur.execute(sql, values)
        return self.cur.fetchone()[0]

    def _insert_affiliation(self, aff_data):
        """Insert or get affiliation"""
        sql = """
            INSERT INTO affiliations (
                scopus_affiliation_id, affiliation_name,
                city, state, country, postal_code
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (scopus_affiliation_id) DO UPDATE SET
                affiliation_name = EXCLUDED.affiliation_name
            RETURNING affiliation_id
        """

        values = (
            aff_data.get("@id"),
            aff_data.get("affilname"),
            aff_data.get("affiliation-city"),
            aff_data.get("state"),
            aff_data.get("affiliation-country"),
            aff_data.get("postal-code"),
        )

        self.cur.execute(sql, values)
        return self.cur.fetchone()[0]

    def _link_paper_author(self, paper_id, author_id, sequence):
        """Link paper to author"""
        # Use DO UPDATE with RETURNING so we always get a paper_author_id
        # whether the row was newly inserted or already existed.
        sql = """
            INSERT INTO paper_authors (paper_id, author_id, author_sequence)
            VALUES (%s, %s, %s)
            ON CONFLICT (paper_id, author_id) DO UPDATE SET
                author_sequence = EXCLUDED.author_sequence
            RETURNING paper_author_id
        """

        self.cur.execute(sql, (paper_id, author_id, sequence))
        result = self.cur.fetchone()
        # Should always return a paper_author_id; but guard defensively.
        return result[0] if result and result[0] is not None else None

    def _link_paper_author_affiliation(self, paper_author_id, affiliation_id):
        """Link paper-author to affiliation"""
        sql = """
            INSERT INTO paper_author_affiliations (paper_author_id, affiliation_id)
            VALUES (%s, %s)
            ON CONFLICT (paper_author_id, affiliation_id) DO NOTHING
        """

        self.cur.execute(sql, (paper_author_id, affiliation_id))

    def _insert_keywords(self, json_data, paper_id):
        """Insert keywords"""
        # Author keywords
        auth_keywords = json_data.get("authkeywords", {})
        if auth_keywords is None:
            auth_keywords = []
        else:
            auth_keywords = auth_keywords.get("author-keyword", [])

        for kw in auth_keywords:
            keyword_id = self._insert_keyword(kw.get("$"), "author")
            self._link_paper_keyword(paper_id, keyword_id, "author")

        # Indexed terms
        idx_terms = json_data.get("idxterms", {})
        if idx_terms is None:
            idx_terms = []
        else:
            idx_terms = idx_terms.get("idxterm", [])
        for term in idx_terms:
            keyword_id = self._insert_keyword(term.get("$"), "indexed")
            self._link_paper_keyword(paper_id, keyword_id, "indexed")

    def _insert_keyword(self, keyword_text, keyword_type):
        """Insert or get keyword"""
        sql = """
            INSERT INTO keywords (keyword, keyword_type)
            VALUES (%s, %s)
            ON CONFLICT (keyword) DO UPDATE SET
                keyword = EXCLUDED.keyword
            RETURNING keyword_id
        """

        self.cur.execute(sql, (keyword_text, keyword_type))
        return self.cur.fetchone()[0]

    def _link_paper_keyword(self, paper_id, keyword_id, keyword_type):
        """Link paper to keyword"""
        sql = """
            INSERT INTO paper_keywords (paper_id, keyword_id, keyword_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (paper_id, keyword_id) DO NOTHING
        """

        self.cur.execute(sql, (paper_id, keyword_id, keyword_type))

    def _insert_subject_areas(self, json_data, paper_id):
        """Insert subject areas"""
        subject_areas = json_data.get("subject-areas", {}).get("subject-area", [])

        for sa in subject_areas:
            subject_id = self._insert_subject_area(sa)
            self._link_paper_subject_area(paper_id, subject_id)

    def _insert_subject_area(self, subject_data):
        """Insert or get subject area"""
        sql = """
            INSERT INTO subject_areas (subject_code, subject_name, subject_abbrev)
            VALUES (%s, %s, %s)
            ON CONFLICT (subject_code) DO UPDATE SET
                subject_name = EXCLUDED.subject_name
            RETURNING subject_area_id
        """

        values = (
            subject_data.get("@code"),
            subject_data.get("$"),
            subject_data.get("@abbrev"),
        )

        self.cur.execute(sql, values)
        return self.cur.fetchone()[0]

    def _link_paper_subject_area(self, paper_id, subject_area_id):
        """Link paper to subject area"""
        sql = """
            INSERT INTO paper_subject_areas (paper_id, subject_area_id)
            VALUES (%s, %s)
            ON CONFLICT (paper_id, subject_area_id) DO NOTHING
        """

        self.cur.execute(sql, (paper_id, subject_area_id))

    def _insert_funding(self, json_data, paper_id):
        """Insert funding information"""
        funding_list = (
            json_data.get("item", {}).get("xocs:meta", {}).get("xocs:funding-list", {})
        )
        funding_sources = funding_list.get("xocs:funding", [])

        if not isinstance(funding_sources, list):
            funding_sources = [funding_sources]

        for funding in funding_sources:
            agency_id = self._insert_funding_agency(funding)

            # Handle multiple grant IDs
            grant_ids = funding.get("xocs:funding-id", [])
            if not isinstance(grant_ids, list):
                grant_ids = [grant_ids]

            if not grant_ids:
                grant_ids = [None]

            for grant in grant_ids:
                grant_id_str = grant.get("$") if isinstance(grant, dict) else grant
                self._link_paper_funding(paper_id, agency_id, grant_id_str)

    def _insert_funding_agency(self, funding_data):
        """Insert or get funding agency"""
        name = funding_data.get("xocs:funding-agency")
        acronym = funding_data.get("xocs:funding-agency-acronym")
        country = funding_data.get("xocs:funding-agency-country")
        scopus_id = funding_data.get("xocs:funding-agency-id")

        # If we have a scopus id, prefer to find existing agency by it first.
        if scopus_id:
            try:
                self.cur.execute(
                    "SELECT agency_id FROM funding_agencies WHERE scopus_agency_id=%s",
                    (scopus_id,),
                )
                row = self.cur.fetchone()
                if row:
                    return row[0]
            except Exception:
                # Continue to insert path if select fails
                pass

            # If name missing, create a placeholder name to satisfy NOT NULL.
            if not name:
                name = f"scopus_agency_{scopus_id}"

            # Try to insert; use INSERT ... SELECT WHERE NOT EXISTS to avoid
            # relying on ON CONFLICT which may reference a non-unique column.
            insert_sql = """
                INSERT INTO funding_agencies (
                    agency_name, agency_acronym, agency_country, scopus_agency_id
                )
                SELECT %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM funding_agencies WHERE scopus_agency_id = %s
                )
                RETURNING agency_id
            """

            self.cur.execute(insert_sql, (name, acronym, country, scopus_id, scopus_id))
            row = self.cur.fetchone()
            if row and row[0] is not None:
                return row[0]

            # Fallback: someone else inserted in the meantime; select and return
            self.cur.execute(
                "SELECT agency_id FROM funding_agencies WHERE scopus_agency_id=%s",
                (scopus_id,),
            )
            row = self.cur.fetchone()
            return row[0] if row else None

        # No scopus id: require a name to insert or lookup
        if not name:
            return None

        # Try to find existing by name + country (null-safe using IS NOT DISTINCT FROM)
        try:
            self.cur.execute(
                "SELECT agency_id FROM funding_agencies WHERE agency_name=%s AND agency_country IS NOT DISTINCT FROM %s",
                (name, country),
            )
            row = self.cur.fetchone()
            if row:
                return row[0]
        except Exception:
            pass

        # Insert and return id. If concurrent insert happens, the subsequent
        # SELECT above would have caught it in many cases; if a race still
        # occurs, let the exception surface so caller can handle/rollback.
        insert_sql = """
            INSERT INTO funding_agencies (
                agency_name, agency_acronym, agency_country, scopus_agency_id
            ) VALUES (%s, %s, %s, %s)
            RETURNING agency_id
        """

        self.cur.execute(insert_sql, (name, acronym, country, None))
        return self.cur.fetchone()[0]

    def _link_paper_funding(self, paper_id, agency_id, grant_id):
        """Link paper to funding"""
        # Guard against attempts to link to a missing agency (None).
        if agency_id is None:
            return

        # Some DB schemas may not have a unique constraint on (paper_id,
        # agency_id, grant_id), so avoid ON CONFLICT. Instead, do a
        # SELECT-first and insert only if the row does not already exist.
        # Use IS NOT DISTINCT FROM to correctly compare NULL grant_id values.
        try:
            self.cur.execute(
                "SELECT 1 FROM paper_funding WHERE paper_id=%s AND agency_id=%s AND (grant_id IS NOT DISTINCT FROM %s)",
                (paper_id, agency_id, grant_id),
            )
            if self.cur.fetchone():
                return
        except Exception:
            # If the select fails for any reason, fall back to attempting an
            # insert; errors will surface to the caller.
            pass

        insert_sql = """
            INSERT INTO paper_funding (paper_id, agency_id, grant_id)
            VALUES (%s, %s, %s)
        """

        self.cur.execute(insert_sql, (paper_id, agency_id, grant_id))

    def _insert_references(self, json_data, paper_id):
        """Insert references"""
        bib_data = json_data.get("item", {}).get("bibrecord", {}).get("tail", {})
        references = bib_data.get("bibliography", {}).get("reference", [])

        for ref in references:
            self._insert_reference(paper_id, ref)

    def _insert_reference(self, paper_id, ref_data):
        """Insert a reference"""
        sql = """
            INSERT INTO reference_papers (
                paper_id, reference_sequence, reference_fulltext,
                cited_year, cited_volume, cited_pages
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (paper_id, reference_sequence) DO NOTHING
        """

        ref_info = ref_data.get("ref-info", {})

        values = (
            paper_id,
            int(ref_data.get("@id", 0)),
            ref_data.get("ref-fulltext"),
            ref_info.get("ref-publicationyear", {}).get("@first"),
            ref_info.get("ref-volisspag", {}).get("voliss", {}).get("@volume"),
            ref_info.get("ref-volisspag", {}).get("pagerange", {}).get("@first"),
        )

        self.cur.execute(sql, values)

    def close(self):
        """Close resources owned by this loader.

        If the loader was given an external connection (`conn`) this method
        will close the cursor but will NOT close the connection (caller
        remains responsible for closing it).
        """
        try:
            self.cur.close()
        except Exception:
            pass

        if self._owns_connection:
            try:
                self.conn.close()
            except Exception:
                pass


# Usage example
if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()
    conn_string = dotenv.get_key(".env", "CONN_STRING")
    # Example 1: loader manages its own connection/transactions (backwards
    # compatible)
    # loader = ScopusDBLoader(conn_string=conn_string)
    # with open("./processing/data/2018/201800000", "r", encoding="utf-8") as f:
    #     data = json.load(f)
    #     paper_id = loader.insert_paper(data["abstracts-retrieval-response"])
    #     print(f"Inserted paper with ID: {paper_id}")
    # loader.close()

    # Example 2: caller provides a connection and manages a transaction that
    # can span multiple inserts. Caller controls commit/rollback.
    conn = psycopg2.connect(conn_string)
    loader2 = ScopusDBLoader(conn=conn)
    try:
        # Suppose we want to insert several papers and commit only at the end
        with open("./processing/data/2018/201800000", "r", encoding="utf-8") as f:
            data = json.load(f)
            pid = loader2.insert_paper(data["abstracts-retrieval-response"])
            print(f"Inserted paper (deferred commit) with ID: {pid}")

        # If all good, commit once for all inserts
        conn.commit()
    except Exception:
        # Caller can decide to abort the whole batch
        conn.rollback()
        raise
    finally:
        loader2.close()
        conn.close()
