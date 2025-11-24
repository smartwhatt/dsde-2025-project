"""CSV exporter for Scopus JSON data.

Extracts data from Scopus JSON files and saves to CSV files representing database tables.
This provides a database-free alternative to handler.py for data export and analysis.

Usage:
    exporter = ScopusCSVExporter(output_dir="./output")
    exporter.export_papers_batch(list_of_json)
    exporter.close()

Output files:
    - sources.csv
    - affiliations.csv
    - authors.csv
    - subject_areas.csv
    - keywords.csv
    - papers.csv
    - paper_authors.csv
    - paper_author_affiliations.csv
    - paper_keywords.csv
    - paper_subject_areas.csv
    - reference_papers.csv
    - funding_agencies.csv
    - paper_funding.csv
"""

import csv
import os
from collections import defaultdict
from typing import Dict, List, Optional, Callable, Set


class ScopusCSVExporter:
    """Exporter that writes Scopus data to CSV files instead of database.

    Contract:
      - Input: list of 'abstracts-retrieval-response' dicts.
      - Output: CSV files representing normalized database tables.
      - Progress: optional callback(progress_count:int, total:int) after each paper.
    """

    def __init__(self, output_dir: str = "./output"):
        """Initialize CSV exporter.

        Args:
            output_dir: Directory where CSV files will be written
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        # Track unique IDs to avoid duplicates
        self._sources_seen: Set[str] = set()
        self._affiliations_seen: Set[str] = set()
        self._authors_seen: Set[str] = set()
        self._subjects_seen: Set[str] = set()
        self._keywords_seen: Set[str] = set()
        self._papers_seen: Set[str] = set()
        self._funding_agencies_seen: Set[str] = set()

        # Counter for generating synthetic IDs
        self._source_id_counter = 1
        self._affiliation_id_counter = 1
        self._author_id_counter = 1
        self._subject_id_counter = 1
        self._keyword_id_counter = 1
        self._paper_id_counter = 1
        self._paper_author_id_counter = 1
        self._funding_agency_id_counter = 1

        # Mapping from natural keys to synthetic IDs
        self._source_id_map: Dict[str, int] = {}
        self._affiliation_id_map: Dict[str, int] = {}
        self._author_id_map: Dict[str, int] = {}
        self._subject_id_map: Dict[str, int] = {}
        self._keyword_id_map: Dict[str, int] = {}
        self._paper_id_map: Dict[str, int] = {}
        self._funding_agency_id_map: Dict[str, int] = {}

        # Initialize CSV files and writers
        self._init_csv_files()

    def _init_csv_files(self):
        """Initialize all CSV files with headers."""
        # Sources
        self.sources_file = open(
            os.path.join(self.output_dir, "sources.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.sources_writer = csv.writer(self.sources_file)
        self.sources_writer.writerow(
            [
                "source_id",
                "source_name",
                "source_abbrev",
                "scopus_source_id",
                "issn_print",
                "issn_electronic",
                "publisher",
                "source_type",
            ]
        )

        # Affiliations
        self.affiliations_file = open(
            os.path.join(self.output_dir, "affiliations.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.affiliations_writer = csv.writer(self.affiliations_file)
        self.affiliations_writer.writerow(
            [
                "affiliation_id",
                "scopus_affiliation_id",
                "affiliation_name",
                "city",
                "state",
                "country",
                "postal_code",
            ]
        )

        # Authors
        self.authors_file = open(
            os.path.join(self.output_dir, "authors.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.authors_writer = csv.writer(self.authors_file)
        self.authors_writer.writerow(
            ["author_id", "auid", "surname", "given_name", "initials", "indexed_name"]
        )

        # Subject Areas
        self.subjects_file = open(
            os.path.join(self.output_dir, "subject_areas.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.subjects_writer = csv.writer(self.subjects_file)
        self.subjects_writer.writerow(
            ["subject_area_id", "subject_code", "subject_name", "subject_abbrev"]
        )

        # Keywords
        self.keywords_file = open(
            os.path.join(self.output_dir, "keywords.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.keywords_writer = csv.writer(self.keywords_file)
        self.keywords_writer.writerow(["keyword_id", "keyword", "keyword_type"])

        # Papers
        self.papers_file = open(
            os.path.join(self.output_dir, "papers.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.papers_writer = csv.writer(self.papers_file)
        self.papers_writer.writerow(
            [
                "paper_id",
                "scopus_id",
                "eid",
                "doi",
                "title",
                "abstract",
                "publication_date",
                "publication_year",
                "source_id",
                "source_type",
                "volume",
                "issue",
                "page_range",
                "start_page",
                "end_page",
                "cited_by_count",
                "open_access",
                "document_type",
                "subtype_description",
            ]
        )

        # Paper Authors
        self.paper_authors_file = open(
            os.path.join(self.output_dir, "paper_authors.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.paper_authors_writer = csv.writer(self.paper_authors_file)
        self.paper_authors_writer.writerow(
            ["paper_author_id", "paper_id", "author_id", "author_sequence"]
        )

        # Paper Author Affiliations
        self.paper_author_affiliations_file = open(
            os.path.join(self.output_dir, "paper_author_affiliations.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.paper_author_affiliations_writer = csv.writer(
            self.paper_author_affiliations_file
        )
        self.paper_author_affiliations_writer.writerow(
            ["paper_author_id", "affiliation_id"]
        )

        # Paper Keywords
        self.paper_keywords_file = open(
            os.path.join(self.output_dir, "paper_keywords.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.paper_keywords_writer = csv.writer(self.paper_keywords_file)
        self.paper_keywords_writer.writerow(["paper_id", "keyword_id", "keyword_type"])

        # Paper Subject Areas
        self.paper_subjects_file = open(
            os.path.join(self.output_dir, "paper_subject_areas.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.paper_subjects_writer = csv.writer(self.paper_subjects_file)
        self.paper_subjects_writer.writerow(["paper_id", "subject_area_id"])

        # Reference Papers
        self.references_file = open(
            os.path.join(self.output_dir, "reference_papers.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.references_writer = csv.writer(self.references_file)
        self.references_writer.writerow(
            [
                "paper_id",
                "reference_sequence",
                "reference_fulltext",
                "cited_year",
                "cited_volume",
                "cited_pages",
            ]
        )

        # Funding Agencies
        self.funding_agencies_file = open(
            os.path.join(self.output_dir, "funding_agencies.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.funding_agencies_writer = csv.writer(self.funding_agencies_file)
        self.funding_agencies_writer.writerow(
            [
                "agency_id",
                "agency_name",
                "agency_acronym",
                "agency_country",
                "scopus_agency_id",
            ]
        )

        # Paper Funding
        self.paper_funding_file = open(
            os.path.join(self.output_dir, "paper_funding.csv"),
            "w",
            newline="",
            encoding="utf-8",
        )
        self.paper_funding_writer = csv.writer(self.paper_funding_file)
        self.paper_funding_writer.writerow(["paper_id", "agency_id", "grant_id"])

    # -------- Public API --------
    def export_papers_batch(
        self,
        json_list: List[dict],
        progress_callback: Optional[Callable[[int, int], None]] = None,
        task_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> List[int]:
        """Export a batch of papers to CSV files.

        Args:
            json_list: List of Scopus JSON objects
            progress_callback: Optional callback(current, total) for progress updates
            task_callback: Optional callback(task_name, current_task, total_tasks)

        Returns:
            List of paper IDs (synthetic IDs generated for this export)
        """
        if not json_list:
            return []

        total = len(json_list)
        total_stages = 7

        # 1. Extract and write dimension tables
        if task_callback:
            task_callback("Extracting metadata", 0, total_stages)
        extraction = self._extract_dimension_sets(json_list)

        if task_callback:
            task_callback("Writing dimension tables", 1, total_stages)
        self._write_dimensions(extraction)

        # 2. Process papers and relational links
        paper_ids = self._process_papers_and_links(
            json_list, progress_callback, task_callback, total_stages
        )

        return paper_ids

    # -------- Extraction Phase --------
    def _extract_dimension_sets(self, json_list: List[dict]):
        """Extract unique dimension entities from JSON data."""
        sources = {}  # scopus_source_id -> tuple of columns
        affiliations = {}  # scopus_affiliation_id -> tuple
        authors = {}  # auid -> tuple
        subject_areas = {}  # subject_code -> tuple
        keywords_author = set()
        keywords_indexed = set()

        for data in json_list:
            core = data.get("coredata", {})

            # ---- Sources ----
            try:
                source_info = data["item"]["bibrecord"]["head"]["source"]
            except Exception:
                source_info = {}
            sc_src = source_info.get("@srcid")
            if sc_src and sc_src not in sources and sc_src not in self._sources_seen:
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
            affs = data.get("affiliation", [])
            if affs and not isinstance(affs, list):
                affs = [affs]
            for aff in affs:
                sc_aff = aff.get("@id")
                if (
                    sc_aff
                    and sc_aff not in affiliations
                    and sc_aff not in self._affiliations_seen
                ):
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
                if auid and auid not in authors and auid not in self._authors_seen:
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
                if (
                    code
                    and code not in subject_areas
                    and code not in self._subjects_seen
                ):
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
                    if val and val not in self._keywords_seen:
                        keywords_author.add(val)
            idx_kw = data.get("idxterms", {})
            if idx_kw:
                terms = idx_kw.get("idxterm", []) or []
                for t in terms:
                    val = (t or {}).get("$") if isinstance(t, dict) else t
                    if val and val not in self._keywords_seen:
                        keywords_indexed.add(val)

        return {
            "sources": sources,
            "affiliations": affiliations,
            "authors": authors,
            "subjects": subject_areas,
            "keywords_author": keywords_author,
            "keywords_indexed": keywords_indexed,
        }

    # -------- Write Dimensions --------
    def _write_dimensions(self, ext):
        """Write dimension tables to CSV files."""
        # Sources
        for sc_src, data in ext["sources"].items():
            if sc_src not in self._sources_seen:
                source_id = self._source_id_counter
                self._source_id_counter += 1
                self._source_id_map[sc_src] = source_id
                self._sources_seen.add(sc_src)
                self.sources_writer.writerow([source_id] + list(data))
                self.sources_file.flush()

        # Affiliations
        for sc_aff, data in ext["affiliations"].items():
            if sc_aff not in self._affiliations_seen:
                aff_id = self._affiliation_id_counter
                self._affiliation_id_counter += 1
                self._affiliation_id_map[sc_aff] = aff_id
                self._affiliations_seen.add(sc_aff)
                self.affiliations_writer.writerow([aff_id] + list(data))
                self.affiliations_file.flush()

        # Authors
        for auid, data in ext["authors"].items():
            if auid not in self._authors_seen:
                author_id = self._author_id_counter
                self._author_id_counter += 1
                self._author_id_map[auid] = author_id
                self._authors_seen.add(auid)
                self.authors_writer.writerow([author_id] + list(data))
                self.authors_file.flush()

        # Subject Areas
        for code, data in ext["subjects"].items():
            if code not in self._subjects_seen:
                subject_id = self._subject_id_counter
                self._subject_id_counter += 1
                self._subject_id_map[code] = subject_id
                self._subjects_seen.add(code)
                self.subjects_writer.writerow([subject_id] + list(data))
                self.subjects_file.flush()

        # Keywords
        all_keywords = []
        for kw in ext["keywords_author"]:
            if kw not in self._keywords_seen:
                all_keywords.append((kw, "author"))
        for kw in ext["keywords_indexed"]:
            if kw not in self._keywords_seen and kw not in ext["keywords_author"]:
                all_keywords.append((kw, "indexed"))

        for kw, kw_type in all_keywords:
            if kw not in self._keywords_seen:
                keyword_id = self._keyword_id_counter
                self._keyword_id_counter += 1
                self._keyword_id_map[kw] = keyword_id
                self._keywords_seen.add(kw)
                self.keywords_writer.writerow([keyword_id, kw, kw_type])
                self.keywords_file.flush()

    # -------- Papers & Relational Links --------
    def _process_papers_and_links(
        self,
        json_list: List[dict],
        progress_callback: Optional[Callable[[int, int], None]],
        task_callback: Optional[Callable[[str, int, int], None]],
        total_stages: int,
    ) -> List[int]:
        """Process papers and write all relational data to CSV files."""
        total = len(json_list)
        paper_ids_ordered = []

        # -------------------- Papers --------------------
        if task_callback:
            task_callback("Processing papers", 2, total_stages)

        paper_author_id_map = {}  # (paper_id, author_id) -> paper_author_id

        for idx, data in enumerate(json_list, 1):
            core = data.get("coredata", {})
            sc_id = core.get("dc:identifier")

            # Skip if already seen
            if sc_id in self._papers_seen:
                paper_ids_ordered.append(self._paper_id_map.get(sc_id))
                if progress_callback:
                    try:
                        progress_callback(idx, total)
                    except Exception:
                        pass
                continue

            # Resolve source
            source_id = self._resolve_source_id(data)

            # Create paper
            pub_date = core.get("prism:coverDate")
            pub_year = int(pub_date[:4]) if pub_date else None

            paper_id = self._paper_id_counter
            self._paper_id_counter += 1
            self._paper_id_map[sc_id] = paper_id
            self._papers_seen.add(sc_id)
            paper_ids_ordered.append(paper_id)

            # Write paper row
            self.papers_writer.writerow(
                [
                    paper_id,
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
                    int(core.get("citedby-count") if core.get("citedby-count") else 0),
                    core.get("openaccess") == "2",
                    core.get("subtype"),
                    core.get("subtypeDescription"),
                ]
            )
            self.papers_file.flush()

            # -------------------- Authors --------------------
            if task_callback:
                task_callback("Processing authors", 3, total_stages)

            authors = data.get("authors", {}).get("author", []) or []
            for author in authors:
                auid = author.get("@auid")
                if not auid:
                    continue

                author_id = self._author_id_map.get(auid)
                if author_id is None:
                    # Fallback: add author if not already in map
                    author_id = self._author_id_counter
                    self._author_id_counter += 1
                    self._author_id_map[auid] = author_id
                    if auid not in self._authors_seen:
                        pref = author.get("preferred-name", {})
                        self.authors_writer.writerow(
                            [
                                author_id,
                                auid,
                                author.get("ce:surname"),
                                pref.get("ce:given-name"),
                                author.get("ce:initials"),
                                author.get("ce:indexed-name"),
                            ]
                        )
                        self.authors_file.flush()
                        self._authors_seen.add(auid)

                # Link paper-author
                seq = int(author.get("@seq", 0))
                paper_author_id = self._paper_author_id_counter
                self._paper_author_id_counter += 1
                paper_author_id_map[(paper_id, author_id)] = paper_author_id

                self.paper_authors_writer.writerow(
                    [paper_author_id, paper_id, author_id, seq]
                )
                self.paper_authors_file.flush()

                # -------------------- Affiliations --------------------
                affs = author.get("affiliation", [])
                if affs and not isinstance(affs, list):
                    affs = [affs]
                for aff in affs:
                    sc_aff = aff.get("@id")
                    if not sc_aff:
                        continue

                    aff_id = self._affiliation_id_map.get(sc_aff)
                    if aff_id is None:
                        # Fallback: add affiliation if not in map
                        aff_id = self._affiliation_id_counter
                        self._affiliation_id_counter += 1
                        self._affiliation_id_map[sc_aff] = aff_id
                        if sc_aff not in self._affiliations_seen:
                            self.affiliations_writer.writerow(
                                [
                                    aff_id,
                                    sc_aff,
                                    aff.get("affilname"),
                                    aff.get("affiliation-city"),
                                    aff.get("state"),
                                    aff.get("affiliation-country"),
                                    aff.get("postal-code"),
                                ]
                            )
                            self.affiliations_file.flush()
                            self._affiliations_seen.add(sc_aff)

                    self.paper_author_affiliations_writer.writerow(
                        [paper_author_id, aff_id]
                    )
                    self.paper_author_affiliations_file.flush()

            # -------------------- Keywords --------------------
            if task_callback:
                task_callback("Processing keywords", 4, total_stages)

            # Author keywords
            auth_kw = data.get("authkeywords", {})
            kws = auth_kw.get("author-keyword", []) if auth_kw else []
            if kws and not isinstance(kws, list):
                kws = [kws]
            for kw in kws:
                val = (kw or {}).get("$") if isinstance(kw, dict) else kw
                if not val:
                    continue
                keyword_id = self._get_or_create_keyword(val, "author")
                self.paper_keywords_writer.writerow([paper_id, keyword_id, "author"])
                self.paper_keywords_file.flush()

            # Indexed terms
            idx_kw = data.get("idxterms", {})
            terms = idx_kw.get("idxterm", []) if idx_kw else []
            for term in terms:
                val = (term or {}).get("$") if isinstance(term, dict) else term
                if not val:
                    continue
                keyword_id = self._get_or_create_keyword(val, "indexed")
                self.paper_keywords_writer.writerow([paper_id, keyword_id, "indexed"])
                self.paper_keywords_file.flush()

            # -------------------- Subject Areas --------------------
            if task_callback:
                task_callback("Processing subjects", 5, total_stages)

            for sa in data.get("subject-areas", {}).get("subject-area", []) or []:
                code = sa.get("@code")
                if not code:
                    continue

                subject_id = self._subject_id_map.get(code)
                if subject_id is None:
                    # Fallback: add subject if not in map
                    subject_id = self._subject_id_counter
                    self._subject_id_counter += 1
                    self._subject_id_map[code] = subject_id
                    if code not in self._subjects_seen:
                        self.subjects_writer.writerow(
                            [subject_id, code, sa.get("$"), sa.get("@abbrev")]
                        )
                        self.subjects_file.flush()
                        self._subjects_seen.add(code)

                self.paper_subjects_writer.writerow([paper_id, subject_id])
                self.paper_subjects_file.flush()

            # -------------------- References --------------------
            if task_callback:
                task_callback("Processing references", 6, total_stages)

            bib = data.get("item", {}).get("bibrecord", {}).get("tail", {})
            if bib is None:
                refs = []
            else:
                refs = bib.get("bibliography", {})
                if refs is None:
                    refs = []
                else:
                    refs = refs.get("reference", []) or []
            if isinstance(refs, dict):
                refs = [refs]

            for r in refs:
                ref_info = r.get("ref-info", {})
                self.references_writer.writerow(
                    [
                        paper_id,
                        int(r.get("@id", 0)),
                        r.get("ref-fulltext"),
                        ref_info.get("ref-publicationyear", {}).get("@first"),
                        ref_info.get("ref-volisspag", {})
                        .get("voliss", {})
                        .get("@volume"),
                        ref_info.get("ref-volisspag", {})
                        .get("pagerange", {})
                        .get("@first"),
                    ]
                )
                self.references_file.flush()

            # -------------------- Funding --------------------
            if task_callback:
                task_callback("Processing funding", 7, total_stages)

            self._process_funding(data, paper_id)

            # Progress callback
            if progress_callback:
                try:
                    progress_callback(idx, total)
                except Exception:
                    pass

        return paper_ids_ordered

    # -------- Helper Methods --------
    def _resolve_source_id(self, json_data) -> Optional[int]:
        """Resolve or create source ID for a paper."""
        try:
            source_info = json_data["item"]["bibrecord"]["head"]["source"]
        except Exception:
            source_info = {}
        sc_src = source_info.get("@srcid")

        if sc_src and sc_src in self._source_id_map:
            return self._source_id_map[sc_src]

        if not sc_src:
            return None

        # Create new source
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

        source_id = self._source_id_counter
        self._source_id_counter += 1
        self._source_id_map[sc_src] = source_id
        self._sources_seen.add(sc_src)

        self.sources_writer.writerow(
            [
                source_id,
                core.get("prism:publicationName"),
                source_info.get("sourcetitle-abbrev"),
                sc_src,
                issn_print,
                issn_elec,
                core.get("dc:publisher"),
                source_info.get("@type"),
            ]
        )
        self.sources_file.flush()

        return source_id

    def _get_or_create_keyword(self, keyword: str, kw_type: str) -> int:
        """Get or create keyword ID."""
        if keyword in self._keyword_id_map:
            return self._keyword_id_map[keyword]

        keyword_id = self._keyword_id_counter
        self._keyword_id_counter += 1
        self._keyword_id_map[keyword] = keyword_id

        if keyword not in self._keywords_seen:
            self.keywords_writer.writerow([keyword_id, keyword, kw_type])
            self.keywords_file.flush()
            self._keywords_seen.add(keyword)

        return keyword_id

    def _process_funding(self, data, paper_id):
        """Process funding information for a paper."""
        funding_list = (
            data.get("item", {}).get("xocs:meta", {}).get("xocs:funding-list", {})
        )
        sources = funding_list.get("xocs:funding", [])
        if sources and not isinstance(sources, list):
            sources = [sources]

        for f in sources or []:
            agency_id = self._resolve_funding_agency(f)
            if agency_id is None:
                continue

            grant_ids = f.get("xocs:funding-id", [])
            if grant_ids and not isinstance(grant_ids, list):
                grant_ids = [grant_ids]
            if not grant_ids:
                grant_ids = [None]

            for g in grant_ids:
                gval = g.get("$") if isinstance(g, dict) else g
                self.paper_funding_writer.writerow([paper_id, agency_id, gval])
                self.paper_funding_file.flush()

    def _resolve_funding_agency(self, f) -> Optional[int]:
        """Resolve or create funding agency ID."""
        sc_id = f.get("xocs:funding-agency-id")
        name = f.get("xocs:funding-agency") or (sc_id and f"scopus_agency_{sc_id}")
        acronym = f.get("xocs:funding-agency-acronym")
        country = f.get("xocs:funding-agency-country")

        # Create lookup key
        if sc_id:
            lookup_key = f"scopus_{sc_id}"
        elif name and country:
            lookup_key = f"{name}_{country}"
        elif name:
            lookup_key = name
        else:
            return None

        if lookup_key in self._funding_agency_id_map:
            return self._funding_agency_id_map[lookup_key]

        # Create new agency
        agency_id = self._funding_agency_id_counter
        self._funding_agency_id_counter += 1
        self._funding_agency_id_map[lookup_key] = agency_id

        if lookup_key not in self._funding_agencies_seen:
            self.funding_agencies_writer.writerow(
                [agency_id, name, acronym, country, sc_id]
            )
            self.funding_agencies_file.flush()
            self._funding_agencies_seen.add(lookup_key)

        return agency_id

    # -------- Resource Management --------
    def close(self):
        """Close all CSV files."""
        files = [
            self.sources_file,
            self.affiliations_file,
            self.authors_file,
            self.subjects_file,
            self.keywords_file,
            self.papers_file,
            self.paper_authors_file,
            self.paper_author_affiliations_file,
            self.paper_keywords_file,
            self.paper_subjects_file,
            self.references_file,
            self.funding_agencies_file,
            self.paper_funding_file,
        ]
        for f in files:
            try:
                f.close()
            except Exception:
                pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
