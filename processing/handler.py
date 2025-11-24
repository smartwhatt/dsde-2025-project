"""High-performance, maintainable batch loader for Scopus JSON data.

Rewrite goals:
  - Separate extraction, bulk upsert, and per-paper linking for clarity.
  - Minimize per-row round trips using execute_values bulk operations.
  - Provide a simple, single public API: `insert_papers_batch` with an optional
    progress callback.
  - Preserve transactional fail-fast semantics and allow caller-managed
    transactions via external connection.
  - Keep an async wrapper for optional concurrency without duplicating logic.

Usage (synchronous):
    loader = ScopusDBLoader(conn_string=CS)
    paper_ids = loader.insert_papers_batch(list_of_json, commit=True)
    loader.close()

Usage (async):
    async_loader = AsyncScopusDBLoader(CS)
    paper_ids = await async_loader.insert_papers_batch_async(list_of_json)
    await async_loader.close()
"""

from collections import defaultdict
from typing import Dict, List, Tuple, Callable, Optional

import psycopg2
from psycopg2.extras import execute_values


class ScopusDBLoader:
    """Loader encapsulating bulk upsert + paper relational inserts.

    Contract:
      - Input: list of 'abstracts-retrieval-response' dicts.
      - Output: list of paper_id ints (inserted or updated).
      - Error: raises on first failing paper (batch rolled back if we own tx).
      - Progress: optional callback(progress_count:int, total:int) after each paper.
    """

    def __init__(
        self,
        conn_string: Optional[str] = None,
        conn=None,
        disable_metadata_upsert: bool = False,
    ):
        if conn is not None:
            self.conn = conn
            self._owns_connection = False
        elif conn_string is not None:
            self.conn = psycopg2.connect(conn_string)
            self._owns_connection = True
        else:
            raise ValueError("Either conn or conn_string must be provided")

        self.cur = self.conn.cursor()
        self._manage_tx = self._owns_connection
        self._disable_metadata_upsert = disable_metadata_upsert

        # Caches for previously resolved dimension rows (scopus ids -> pk ids)
        self._src_cache: Dict[str, int] = {}
        self._aff_cache: Dict[str, int] = {}
        self._author_cache: Dict[str, int] = {}
        self._subject_cache: Dict[str, int] = {}
        self._keyword_cache: Dict[str, int] = {}

    # -------- Public API --------
    def insert_papers_batch(
        self,
        json_list: List[dict],
        commit: bool = True,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        task_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> List[int]:
        if not json_list:
            return []

        own_tx = self._manage_tx and commit
        try:
            # 1. Bulk upsert shared metadata (sources, affiliations, authors, subjects, keywords)
            if task_callback:
                task_callback("Extracting metadata", 0, 2)
            extraction = self._extract_dimension_sets(json_list)
            if task_callback:
                task_callback("Upserting metadata", 1, 2)
            self._bulk_upsert_dimensions(extraction, task_callback)
            # 2. Bulk insert papers & relational link tables
            paper_ids = self._bulk_insert_papers_and_links(
                json_list, progress_callback, task_callback
            )
            if own_tx:
                self.conn.commit()
            return paper_ids
        except Exception:
            if own_tx:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
            raise

    # -------- Extraction Phase --------
    def _extract_dimension_sets(self, json_list: List[dict]):
        sources = {}  # scopus_source_id -> tuple of columns
        affiliations = {}  # scopus_affiliation_id -> tuple
        authors = {}  # auid -> tuple
        subject_areas = {}  # subject_code -> tuple
        keywords_author = set()
        keywords_indexed = set()

        if self._disable_metadata_upsert:
            # Skip only sources & affiliations bulk stage when disabled
            skip_sources_affs = True
        else:
            skip_sources_affs = False

        for data in json_list:
            core = data.get("coredata", {})
            # ---- Sources ----
            if not skip_sources_affs:
                try:
                    source_info = data["item"]["bibrecord"]["head"]["source"]
                except Exception:
                    source_info = {}
                sc_src = source_info.get("@srcid")
                if sc_src and sc_src not in sources:
                    issn_list = source_info.get("issn", [])
                    issn_print, issn_elec = None, None
                    if isinstance(issn_list, list):
                        for issn in issn_list:
                            t = issn.get("@type")
                            if t == "print":
                                issn_print = issn.get("$")
                            elif t == "electronic":
                                issn_elec = issn.get("$")
                    sources[sc_src] = (
                        core.get("prism:publicationName"),
                        source_info.get("sourcetitle-abbrev"),
                        sc_src,
                        issn_print,
                        issn_elec,
                        core.get("dc:publisher"),
                        source_info.get("@type"),
                    )

            # ---- Affiliations ----
            if not skip_sources_affs:
                affs = data.get("affiliation", [])
                if affs and not isinstance(affs, list):
                    affs = [affs]
                for aff in affs:
                    sc_aff = aff.get("@id")
                    if sc_aff and sc_aff not in affiliations:
                        affiliations[sc_aff] = (
                            sc_aff,
                            aff.get("affilname"),
                            aff.get("affiliation-city"),
                            aff.get("state"),
                            aff.get("affiliation-country"),
                            aff.get("postal-code"),
                        )

            # ---- Authors ----
            for author in data.get("authors", {}).get("author", []) or []:
                auid = author.get("@auid")
                if auid and auid not in authors:
                    pref = author.get("preferred-name", {})
                    authors[auid] = (
                        auid,
                        author.get("ce:surname"),
                        pref.get("ce:given-name"),
                        author.get("ce:initials"),
                        author.get("ce:indexed-name"),
                    )

            # ---- Subject Areas ----
            for sa in data.get("subject-areas", {}).get("subject-area", []) or []:
                code = sa.get("@code")
                if code and code not in subject_areas:
                    subject_areas[code] = (
                        code,
                        sa.get("$"),
                        sa.get("@abbrev"),
                    )

            # ---- Keywords ----
            auth_kw = data.get("authkeywords", {})
            if auth_kw:
                kws = auth_kw.get("author-keyword", []) or []
                if not isinstance(kws, list):
                    kws = [kws]
                for k in kws:
                    val = (k or {}).get("$") if isinstance(k, dict) else k
                    if val:
                        keywords_author.add(val)
            idx_kw = data.get("idxterms", {})
            if idx_kw:
                terms = idx_kw.get("idxterm", []) or []
                for t in terms:
                    val = (t or {}).get("$") if isinstance(t, dict) else t
                    if val:
                        keywords_indexed.add(val)

        return {
            "sources": sources,
            "affiliations": affiliations,
            "authors": authors,
            "subjects": subject_areas,
            "keywords_author": keywords_author,
            "keywords_indexed": keywords_indexed,
            "skip_src_aff": skip_sources_affs,
        }

    # -------- Bulk Upserts --------
    def _bulk_upsert_dimensions(
        self, ext, task_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        total_tasks = 5  # sources, affiliations, authors, subjects, keywords
        current_task = 0

        # Sources
        if not ext["skip_src_aff"] and ext["sources"]:
            if task_callback:
                task_callback(
                    f"Upserting sources ({len(ext['sources'])})",
                    current_task,
                    total_tasks,
                )
            sql = (
                "INSERT INTO sources (source_name, source_abbrev, scopus_source_id, issn_print, issn_electronic, publisher, source_type)"
                " VALUES %s ON CONFLICT (scopus_source_id) DO UPDATE SET"
                " source_name = COALESCE(EXCLUDED.source_name, sources.source_name),"
                " source_abbrev = COALESCE(EXCLUDED.source_abbrev, sources.source_abbrev),"
                " issn_print = COALESCE(EXCLUDED.issn_print, sources.issn_print),"
                " issn_electronic = COALESCE(EXCLUDED.issn_electronic, sources.issn_electronic),"
                " publisher = COALESCE(EXCLUDED.publisher, sources.publisher),"
                " source_type = COALESCE(EXCLUDED.source_type, sources.source_type)"
                " RETURNING source_id, scopus_source_id"
            )
            execute_values(self.cur, sql, list(ext["sources"].values()))
            for sid, scid in self.cur.fetchall():
                self._src_cache[str(scid)] = sid
        current_task += 1

        # Affiliations
        if not ext["skip_src_aff"] and ext["affiliations"]:
            if task_callback:
                task_callback(
                    f"Upserting affiliations ({len(ext['affiliations'])})",
                    current_task,
                    total_tasks,
                )
            sql = (
                "INSERT INTO affiliations (scopus_affiliation_id, affiliation_name, city, state, country, postal_code)"
                " VALUES %s ON CONFLICT (scopus_affiliation_id) DO UPDATE SET"
                " affiliation_name = COALESCE(EXCLUDED.affiliation_name, affiliations.affiliation_name),"
                " city = COALESCE(EXCLUDED.city, affiliations.city),"
                " state = COALESCE(EXCLUDED.state, affiliations.state),"
                " country = COALESCE(EXCLUDED.country, affiliations.country),"
                " postal_code = COALESCE(EXCLUDED.postal_code, affiliations.postal_code)"
                " RETURNING affiliation_id, scopus_affiliation_id"
            )
            execute_values(self.cur, sql, list(ext["affiliations"].values()))
            for aid, scid in self.cur.fetchall():
                self._aff_cache[str(scid)] = aid
        current_task += 1

        # Authors
        if ext["authors"]:
            if task_callback:
                task_callback(
                    f"Upserting authors ({len(ext['authors'])})",
                    current_task,
                    total_tasks,
                )
            sql = (
                "INSERT INTO authors (auid, surname, given_name, initials, indexed_name)"
                " VALUES %s ON CONFLICT (auid) DO UPDATE SET"
                " surname = COALESCE(EXCLUDED.surname, authors.surname)"
                " RETURNING author_id, auid"
            )
            execute_values(self.cur, sql, list(ext["authors"].values()))
            for aid, auid in self.cur.fetchall():
                self._author_cache[str(auid)] = aid
        current_task += 1

        # Subject areas
        if ext["subjects"]:
            if task_callback:
                task_callback(
                    f"Upserting subjects ({len(ext['subjects'])})",
                    current_task,
                    total_tasks,
                )
            sql = (
                "INSERT INTO subject_areas (subject_code, subject_name, subject_abbrev)"
                " VALUES %s ON CONFLICT (subject_code) DO UPDATE SET"
                " subject_name = COALESCE(EXCLUDED.subject_name, subject_areas.subject_name)"
                " RETURNING subject_area_id, subject_code"
            )
            execute_values(self.cur, sql, list(ext["subjects"].values()))
            for sid, code in self.cur.fetchall():
                self._subject_cache[str(code)] = sid
        current_task += 1

        # Keywords (author + indexed share same PK by keyword string)
        all_keywords = []
        for kw in ext["keywords_author"]:
            all_keywords.append((kw, "author"))
        for kw in ext["keywords_indexed"]:
            if kw not in ext["keywords_author"]:
                all_keywords.append((kw, "indexed"))
        if all_keywords:
            if task_callback:
                task_callback(
                    f"Upserting keywords ({len(all_keywords)})",
                    current_task,
                    total_tasks,
                )
            sql = (
                "INSERT INTO keywords (keyword, keyword_type) VALUES %s"
                " ON CONFLICT (keyword) DO UPDATE SET keyword = EXCLUDED.keyword"
                " RETURNING keyword_id, keyword"
            )
            execute_values(self.cur, sql, all_keywords)
            for kid, kw in self.cur.fetchall():
                self._keyword_cache[str(kw)] = kid
        current_task += 1

    # -------- Paper & Relational Inserts --------
    def _bulk_insert_papers_and_links(
        self,
        json_list: List[dict],
        progress_callback: Optional[Callable[[int, int], None]],
        task_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> List[int]:
        """High-performance bulk path replacing per-row inserts.

        Steps:
          1. Gather per-paper core values & resolve source_ids (fallback insert if missing).
          2. Bulk insert/update papers returning mapping (scopus_id -> paper_id).
          3. Gather and bulk insert paper_authors returning mapping (paper_id, author_id -> paper_author_id).
          4. Gather and bulk insert paper_author_affiliations.
          5. Gather and bulk insert paper_keywords.
          6. Gather and bulk insert paper_subject_areas.
          7. Gather and bulk insert reference_papers.
          8. Funding remains row-wise (low volume / variable uniqueness).
          9. Emit progress callback after parsing each paper (fast, keeps UI responsive).
        """

        total = len(json_list)
        total_stages = (
            7  # papers, authors, affiliations, keywords, subjects, references, funding
        )

        # -------------------- Papers --------------------
        if task_callback:
            task_callback("Gathering paper data", 0, total_stages)
        paper_core_rows = []  # list of tuple values for papers insert
        scopus_ids_in_order = []
        source_ids_needed = []
        for idx, data in enumerate(json_list, 1):
            core = data.get("coredata", {})
            # source resolve (may execute rare single insert)
            source_id = self._resolve_source_id(data)
            source_ids_needed.append(source_id)
            pub_date = core.get("prism:coverDate")
            pub_year = int(pub_date[:4]) if pub_date else None
            sc_id = core.get("dc:identifier")
            scopus_ids_in_order.append(sc_id)
            paper_core_rows.append(
                (
                    sc_id,
                    core.get("eid"),
                    core.get("prism:doi"),
                    core.get("dc:title"),
                    core.get("dc:description"),
                    pub_date,
                    pub_year,
                    source_id,
                    core.get("prism:aggregationType"),
                    core.get("prism:volume"),
                    core.get("prism:issueIdentifier"),
                    core.get("prism:pageRange"),
                    core.get("prism:startingPage"),
                    core.get("prism:endingPage"),
                    int(core.get("citedby-count", 0)),
                    core.get("openaccess") == "2",
                    core.get("subtype"),
                    core.get("subtypeDescription"),
                )
            )
            if progress_callback:
                try:
                    progress_callback(idx, total)
                except Exception:
                    pass

        paper_id_map = {}  # scopus_id -> paper_id
        if paper_core_rows:
            if task_callback:
                task_callback(
                    f"Inserting papers ({len(paper_core_rows)})", 1, total_stages
                )
            sql_papers = (
                "INSERT INTO papers (scopus_id, eid, doi, title, abstract, publication_date, publication_year, source_id, source_type, volume, issue, page_range, start_page, end_page, cited_by_count, open_access, document_type, subtype_description)"
                " VALUES %s ON CONFLICT (scopus_id) DO UPDATE SET cited_by_count = EXCLUDED.cited_by_count, updated_at = CURRENT_TIMESTAMP RETURNING scopus_id, paper_id"
            )
            execute_values(self.cur, sql_papers, paper_core_rows)
            for scid, pid in self.cur.fetchall():
                paper_id_map[scid] = pid

        # Ordered list of paper_ids aligned to input order
        paper_ids_ordered = [paper_id_map.get(scid) for scid in scopus_ids_in_order]

        # -------------------- Authors + Affiliations --------------------
        if task_callback:
            task_callback("Gathering author data", 2, total_stages)
        paper_author_rows = []  # (paper_id, author_id, sequence)
        author_aff_rows_pending = (
            []
        )  # (scopus_aff_id, paper_scopus_id, author_auid) temporary to resolve after insert
        for data, pid in zip(json_list, paper_ids_ordered):
            if not pid:
                continue
            authors = data.get("authors", {}).get("author", []) or []
            for author in authors:
                auid = author.get("@auid")
                if not auid:
                    continue
                author_id = self._author_cache.get(auid)
                if author_id is None:
                    # Rare fallback insert
                    pref = author.get("preferred-name", {})
                    self.cur.execute(
                        "INSERT INTO authors (auid, surname, given_name, initials, indexed_name) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (auid) DO UPDATE SET surname = COALESCE(EXCLUDED.surname, authors.surname) RETURNING author_id",
                        (
                            auid,
                            author.get("ce:surname"),
                            pref.get("ce:given-name"),
                            author.get("ce:initials"),
                            author.get("ce:indexed-name"),
                        ),
                    )
                    author_id = self.cur.fetchone()[0]
                    self._author_cache[auid] = author_id
                seq = int(author.get("@seq", 0))
                paper_author_rows.append((pid, author_id, seq))
                affs = author.get("affiliation", [])
                if affs and not isinstance(affs, list):
                    affs = [affs]
                for aff in affs:
                    sc_aff = aff.get("@id")
                    if sc_aff:
                        author_aff_rows_pending.append((sc_aff, pid, author_id))

        paper_author_id_map = {}  # (paper_id, author_id) -> paper_author_id
        if paper_author_rows:
            if task_callback:
                task_callback(
                    f"Linking authors ({len(paper_author_rows)})", 3, total_stages
                )
            sql_pa = "INSERT INTO paper_authors (paper_id, author_id, author_sequence) VALUES %s ON CONFLICT (paper_id, author_id) DO UPDATE SET author_sequence = EXCLUDED.author_sequence RETURNING paper_author_id, paper_id, author_id"
            execute_values(self.cur, sql_pa, paper_author_rows)
            for pa_id, p_id, a_id in self.cur.fetchall():
                paper_author_id_map[(p_id, a_id)] = pa_id

        # Affiliations linking
        if task_callback:
            task_callback("Linking affiliations", 3, total_stages)
        paa_rows = []  # (paper_author_id, affiliation_id)
        for sc_aff, p_id, a_id in author_aff_rows_pending:
            if p_id is None:
                continue
            aff_id = self._aff_cache.get(sc_aff)
            if aff_id is None:
                continue
            pa_id = paper_author_id_map.get((p_id, a_id))
            if pa_id is None:
                continue
            paa_rows.append((pa_id, aff_id))
        if paa_rows:
            if task_callback:
                task_callback(
                    f"Inserting affiliations ({len(paa_rows)})", 3, total_stages
                )
            sql_paa = "INSERT INTO paper_author_affiliations (paper_author_id, affiliation_id) VALUES %s ON CONFLICT (paper_author_id, affiliation_id) DO NOTHING"
            execute_values(self.cur, sql_paa, paa_rows)

        # -------------------- Keywords --------------------
        if task_callback:
            task_callback("Gathering keywords", 4, total_stages)
        keyword_link_rows = []  # (paper_id, keyword_id, keyword_type)
        for data, pid in zip(json_list, paper_ids_ordered):
            if not pid:
                continue
            # Author keywords
            auth_kw = data.get("authkeywords", {})
            kws = auth_kw.get("author-keyword", []) if auth_kw else []
            if kws and not isinstance(kws, list):
                kws = [kws]
            for kw in kws:
                val = (kw or {}).get("$") if isinstance(kw, dict) else kw
                if not val:
                    continue
                kid = self._keyword_cache.get(val)
                if kid is None:
                    kid = self._insert_keyword_single(val, "author")
                keyword_link_rows.append((pid, kid, "author"))
            # Indexed terms
            idx_kw = data.get("idxterms", {})
            terms = idx_kw.get("idxterm", []) if idx_kw else []
            for term in terms:
                val = (term or {}).get("$") if isinstance(term, dict) else term
                if not val:
                    continue
                kid = self._keyword_cache.get(val)
                if kid is None:
                    kid = self._insert_keyword_single(val, "indexed")
                keyword_link_rows.append((pid, kid, "indexed"))
        if keyword_link_rows:
            if task_callback:
                task_callback(
                    f"Linking keywords ({len(keyword_link_rows)})", 4, total_stages
                )
            sql_kw = "INSERT INTO paper_keywords (paper_id, keyword_id, keyword_type) VALUES %s ON CONFLICT (paper_id, keyword_id) DO NOTHING"
            execute_values(self.cur, sql_kw, keyword_link_rows)

        # -------------------- Subject Areas --------------------
        if task_callback:
            task_callback("Gathering subjects", 5, total_stages)
        subject_link_rows = []  # (paper_id, subject_area_id)
        for data, pid in zip(json_list, paper_ids_ordered):
            if not pid:
                continue
            for sa in data.get("subject-areas", {}).get("subject-area", []) or []:
                code = sa.get("@code")
                if not code:
                    continue
                sid = self._subject_cache.get(code)
                if sid is None:
                    # Rare fallback
                    self.cur.execute(
                        "INSERT INTO subject_areas (subject_code, subject_name, subject_abbrev) VALUES (%s,%s,%s) ON CONFLICT (subject_code) DO UPDATE SET subject_name = COALESCE(EXCLUDED.subject_name, subject_areas.subject_name) RETURNING subject_area_id",
                        (code, sa.get("$"), sa.get("@abbrev")),
                    )
                    sid = self.cur.fetchone()[0]
                    self._subject_cache[code] = sid
                subject_link_rows.append((pid, sid))
        if subject_link_rows:
            if task_callback:
                task_callback(
                    f"Linking subjects ({len(subject_link_rows)})", 5, total_stages
                )
            sql_subj = "INSERT INTO paper_subject_areas (paper_id, subject_area_id) VALUES %s ON CONFLICT (paper_id, subject_area_id) DO NOTHING"
            execute_values(self.cur, sql_subj, subject_link_rows)

        # -------------------- References --------------------
        if task_callback:
            task_callback("Gathering references", 6, total_stages)
        reference_rows = (
            []
        )  # (paper_id, reference_sequence, reference_fulltext, cited_year, cited_volume, cited_pages)
        for data, pid in zip(json_list, paper_ids_ordered):
            if not pid:
                continue
            bib = data.get("item", {}).get("bibrecord", {}).get("tail", {})
            refs = bib.get("bibliography", {}).get("reference", []) or []
            for r in refs:
                ref_info = r.get("ref-info", {})
                reference_rows.append(
                    (
                        pid,
                        int(r.get("@id", 0)),
                        r.get("ref-fulltext"),
                        ref_info.get("ref-publicationyear", {}).get("@first"),
                        ref_info.get("ref-volisspag", {})
                        .get("voliss", {})
                        .get("@volume"),
                        ref_info.get("ref-volisspag", {})
                        .get("pagerange", {})
                        .get("@first"),
                    )
                )
        if reference_rows:
            if task_callback:
                task_callback(
                    f"Inserting references ({len(reference_rows)})", 6, total_stages
                )
            sql_refs = "INSERT INTO reference_papers (paper_id, reference_sequence, reference_fulltext, cited_year, cited_volume, cited_pages) VALUES %s ON CONFLICT (paper_id, reference_sequence) DO NOTHING"
            execute_values(self.cur, sql_refs, reference_rows)

        # -------------------- Funding (row-wise retained) --------------------
        if task_callback:
            task_callback("Processing funding", 7, total_stages)
        for data, pid in zip(json_list, paper_ids_ordered):
            if not pid:
                continue
            self._insert_funding(data, pid)

        return paper_ids_ordered

    # -------- Helpers (source, paper, authors, etc.) --------
    def _resolve_source_id(self, json_data) -> Optional[int]:
        try:
            source_info = json_data["item"]["bibrecord"]["head"]["source"]
        except Exception:
            source_info = {}
        sc_src = source_info.get("@srcid")
        if sc_src and sc_src in self._src_cache:
            return self._src_cache[sc_src]
        # Fallback minimal insert (rare if bulk upsert ran)
        core = json_data.get("coredata", {})
        issn_list = source_info.get("issn", [])
        issn_print, issn_elec = None, None
        if isinstance(issn_list, list):
            for issn in issn_list:
                t = issn.get("@type")
                if t == "print":
                    issn_print = issn.get("$")
                elif t == "electronic":
                    issn_elec = issn.get("$")
        sql = (
            "INSERT INTO sources (source_name, source_abbrev, scopus_source_id, issn_print, issn_electronic, publisher, source_type)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (scopus_source_id) DO UPDATE SET"
            " source_name = COALESCE(EXCLUDED.source_name, sources.source_name)"
            " RETURNING source_id"
        )
        self.cur.execute(
            sql,
            (
                core.get("prism:publicationName"),
                source_info.get("sourcetitle-abbrev"),
                sc_src,
                issn_print,
                issn_elec,
                core.get("dc:publisher"),
                source_info.get("@type"),
            ),
        )
        sid = self.cur.fetchone()[0]
        if sc_src:
            self._src_cache[sc_src] = sid
        return sid

    def _insert_paper_row(self, core, source_id) -> int:
        pub_date = core.get("prism:coverDate")
        pub_year = int(pub_date[:4]) if pub_date else None
        sql = (
            "INSERT INTO papers (scopus_id, eid, doi, title, abstract, publication_date, publication_year, source_id, source_type, volume, issue, page_range, start_page, end_page, cited_by_count, open_access, document_type, subtype_description)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            " ON CONFLICT (scopus_id) DO UPDATE SET cited_by_count = EXCLUDED.cited_by_count, updated_at = CURRENT_TIMESTAMP"
            " RETURNING paper_id"
        )
        values = (
            core.get("dc:identifier"),
            core.get("eid"),
            core.get("prism:doi"),
            core.get("dc:title"),
            core.get("dc:description"),
            pub_date,
            pub_year,
            source_id,
            core.get("prism:aggregationType"),
            core.get("prism:volume"),
            core.get("prism:issueIdentifier"),
            core.get("prism:pageRange"),
            core.get("prism:startingPage"),
            core.get("prism:endingPage"),
            int(core.get("citedby-count", 0)),
            core.get("openaccess") == "2",
            core.get("subtype"),
            core.get("subtypeDescription"),
        )
        self.cur.execute(sql, values)
        return self.cur.fetchone()[0]

    def _insert_authors_links(self, data, paper_id):
        authors = data.get("authors", {}).get("author", []) or []
        for author in authors:
            auid = author.get("@auid")
            if not auid:
                continue
            author_id = self._author_cache.get(auid)
            if author_id is None:
                # Should rarely happen if bulk upsert succeeded; fallback insert.
                pref = author.get("preferred-name", {})
                sql = (
                    "INSERT INTO authors (auid, surname, given_name, initials, indexed_name) VALUES (%s,%s,%s,%s,%s)"
                    " ON CONFLICT (auid) DO UPDATE SET surname = COALESCE(EXCLUDED.surname, authors.surname) RETURNING author_id"
                )
                self.cur.execute(
                    sql,
                    (
                        auid,
                        author.get("ce:surname"),
                        pref.get("ce:given-name"),
                        author.get("ce:initials"),
                        author.get("ce:indexed-name"),
                    ),
                )
                author_id = self.cur.fetchone()[0]
                self._author_cache[auid] = author_id
            # Link paper-author
            seq = int(author.get("@seq", 0))
            sql_link = (
                "INSERT INTO paper_authors (paper_id, author_id, author_sequence) VALUES (%s,%s,%s)"
                " ON CONFLICT (paper_id, author_id) DO UPDATE SET author_sequence = EXCLUDED.author_sequence RETURNING paper_author_id"
            )
            self.cur.execute(sql_link, (paper_id, author_id, seq))
            paper_author_id = self.cur.fetchone()[0]
            # Link affiliations
            affs = author.get("affiliation", [])
            if affs and not isinstance(affs, list):
                affs = [affs]
            for aff in affs:
                sc_aff = aff.get("@id")
                if sc_aff and sc_aff in self._aff_cache:
                    sql_aff = (
                        "INSERT INTO paper_author_affiliations (paper_author_id, affiliation_id) VALUES (%s,%s)"
                        " ON CONFLICT (paper_author_id, affiliation_id) DO NOTHING"
                    )
                    self.cur.execute(
                        sql_aff, (paper_author_id, self._aff_cache[sc_aff])
                    )

    def _insert_keywords_links(self, data, paper_id):
        # Author keywords
        auth_kw = data.get("authkeywords", {})
        kws = auth_kw.get("author-keyword", []) if auth_kw else []
        if kws and not isinstance(kws, list):
            kws = [kws]
        for kw in kws:
            val = (kw or {}).get("$") if isinstance(kw, dict) else kw
            if not val:
                continue
            kid = self._keyword_cache.get(val)
            if kid is None:
                kid = self._insert_keyword_single(val, "author")
            self._link_keyword(paper_id, kid, "author")
        # Indexed terms
        idx_kw = data.get("idxterms", {})
        terms = idx_kw.get("idxterm", []) if idx_kw else []
        for term in terms:
            val = (term or {}).get("$") if isinstance(term, dict) else term
            if not val:
                continue
            kid = self._keyword_cache.get(val)
            if kid is None:
                kid = self._insert_keyword_single(val, "indexed")
            self._link_keyword(paper_id, kid, "indexed")

    def _insert_keyword_single(self, text, kw_type) -> int:
        sql = (
            "INSERT INTO keywords (keyword, keyword_type) VALUES (%s,%s)"
            " ON CONFLICT (keyword) DO UPDATE SET keyword = EXCLUDED.keyword RETURNING keyword_id"
        )
        self.cur.execute(sql, (text, kw_type))
        kid = self.cur.fetchone()[0]
        self._keyword_cache[text] = kid
        return kid

    def _link_keyword(self, paper_id, keyword_id, kw_type):
        sql = (
            "INSERT INTO paper_keywords (paper_id, keyword_id, keyword_type) VALUES (%s,%s,%s)"
            " ON CONFLICT (paper_id, keyword_id) DO NOTHING"
        )
        self.cur.execute(sql, (paper_id, keyword_id, kw_type))

    def _insert_subject_links(self, data, paper_id):
        for sa in data.get("subject-areas", {}).get("subject-area", []) or []:
            code = sa.get("@code")
            if not code:
                continue
            sid = self._subject_cache.get(code)
            if sid is None:
                # Rare fallback
                sql = (
                    "INSERT INTO subject_areas (subject_code, subject_name, subject_abbrev) VALUES (%s,%s,%s)"
                    " ON CONFLICT (subject_code) DO UPDATE SET subject_name = COALESCE(EXCLUDED.subject_name, subject_areas.subject_name) RETURNING subject_area_id"
                )
                self.cur.execute(sql, (code, sa.get("$"), sa.get("@abbrev")))
                sid = self.cur.fetchone()[0]
                self._subject_cache[code] = sid
            link_sql = (
                "INSERT INTO paper_subject_areas (paper_id, subject_area_id) VALUES (%s,%s)"
                " ON CONFLICT (paper_id, subject_area_id) DO NOTHING"
            )
            self.cur.execute(link_sql, (paper_id, sid))

    def _insert_funding(self, data, paper_id):
        funding_list = (
            data.get("item", {}).get("xocs:meta", {}).get("xocs:funding-list", {})
        )
        sources = funding_list.get("xocs:funding", [])
        if sources and not isinstance(sources, list):
            sources = [sources]
        for f in sources or []:
            agency_id = self._resolve_funding_agency(f)
            grant_ids = f.get("xocs:funding-id", [])
            if grant_ids and not isinstance(grant_ids, list):
                grant_ids = [grant_ids]
            if not grant_ids:
                grant_ids = [None]
            for g in grant_ids:
                gval = g.get("$") if isinstance(g, dict) else g
                self._link_funding(paper_id, agency_id, gval)

    def _resolve_funding_agency(self, f) -> Optional[int]:
        sc_id = f.get("xocs:funding-agency-id")
        name = f.get("xocs:funding-agency") or (sc_id and f"scopus_agency_{sc_id}")
        acronym = f.get("xocs:funding-agency-acronym")
        country = f.get("xocs:funding-agency-country")
        if sc_id:
            try:
                self.cur.execute(
                    "SELECT agency_id FROM funding_agencies WHERE scopus_agency_id=%s",
                    (sc_id,),
                )
                row = self.cur.fetchone()
                if row:
                    return row[0]
            except Exception:
                pass
            sql = (
                "INSERT INTO funding_agencies (agency_name, agency_acronym, agency_country, scopus_agency_id)"
                " SELECT %s,%s,%s,%s WHERE NOT EXISTS (SELECT 1 FROM funding_agencies WHERE scopus_agency_id=%s)"
                " RETURNING agency_id"
            )
            self.cur.execute(sql, (name, acronym, country, sc_id, sc_id))
            row = self.cur.fetchone()
            if row:
                return row[0]
            # Fallback select
            self.cur.execute(
                "SELECT agency_id FROM funding_agencies WHERE scopus_agency_id=%s",
                (sc_id,),
            )
            row = self.cur.fetchone()
            return row[0] if row else None
        # No scopus id, attempt lookup by name + country
        if not name:
            return None
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
        sql = "INSERT INTO funding_agencies (agency_name, agency_acronym, agency_country, scopus_agency_id) VALUES (%s,%s,%s,%s) RETURNING agency_id"
        self.cur.execute(sql, (name, acronym, country, None))
        return self.cur.fetchone()[0]

    def _link_funding(self, paper_id, agency_id, grant_id):
        if agency_id is None:
            return
        try:
            self.cur.execute(
                "SELECT 1 FROM paper_funding WHERE paper_id=%s AND agency_id=%s AND (grant_id IS NOT DISTINCT FROM %s)",
                (paper_id, agency_id, grant_id),
            )
            if self.cur.fetchone():
                return
        except Exception:
            pass
        self.cur.execute(
            "INSERT INTO paper_funding (paper_id, agency_id, grant_id) VALUES (%s,%s,%s)",
            (paper_id, agency_id, grant_id),
        )

    def _insert_references(self, data, paper_id):
        bib = data.get("item", {}).get("bibrecord", {}).get("tail", {})
        refs = bib.get("bibliography", {}).get("reference", []) or []
        if not refs:
            return
        rows = []
        for r in refs:
            ref_info = r.get("ref-info", {})
            rows.append(
                (
                    paper_id,
                    int(r.get("@id", 0)),
                    r.get("ref-fulltext"),
                    ref_info.get("ref-publicationyear", {}).get("@first"),
                    ref_info.get("ref-volisspag", {}).get("voliss", {}).get("@volume"),
                    ref_info.get("ref-volisspag", {})
                    .get("pagerange", {})
                    .get("@first"),
                )
            )
        sql = (
            "INSERT INTO reference_papers (paper_id, reference_sequence, reference_fulltext, cited_year, cited_volume, cited_pages)"
            " VALUES %s ON CONFLICT (paper_id, reference_sequence) DO NOTHING"
        )
        execute_values(self.cur, sql, rows)

    # -------- Resource Management --------
    def close(self):
        try:
            self.cur.close()
        except Exception:
            pass
        if self._owns_connection:
            try:
                self.conn.close()
            except Exception:
                pass


class AsyncScopusDBLoader:
    """Thin async wrapper using a thread pool to run the synchronous loader."""

    def __init__(
        self,
        conn_string: str,
        max_workers: int = 8,
        disable_metadata_upsert: bool = False,
    ):
        import concurrent.futures

        self.conn_string = conn_string
        self.disable_metadata_upsert = disable_metadata_upsert
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    async def insert_papers_batch_async(
        self,
        json_list: List[dict],
        commit: bool = True,
        progress_callback=None,
        task_callback=None,
    ) -> List[int]:
        import asyncio

        loop = asyncio.get_running_loop()

        def _worker(batch):
            loader = ScopusDBLoader(
                conn_string=self.conn_string,
                disable_metadata_upsert=self.disable_metadata_upsert,
            )
            try:
                return loader.insert_papers_batch(
                    batch,
                    commit=commit,
                    progress_callback=progress_callback,
                    task_callback=task_callback,
                )
            finally:
                loader.close()

        return await loop.run_in_executor(self._executor, _worker, json_list)

    async def close(self):
        import asyncio

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._executor.shutdown, True)
