from pathlib import Path

import feedparser
import pickle

from zotero_arxiv_daily.retriever.arxiv_retriever import ArxivPaperItem, ArxivRetriever
from zotero_arxiv_daily.utils import extract_markdown_from_pdf_with_timeout

def test_arxiv_retriever(config, monkeypatch):

    parsed_result = feedparser.parse("tests/retriever/arxiv_rss_example.xml")
    raw_parser = feedparser.parse
    def mock_feedparser_parse(url):
        if url == f"https://rss.arxiv.org/atom/{'+'.join(config.source.arxiv.category)}":
            return parsed_result
        return raw_parser(url)
    monkeypatch.setattr(feedparser, "parse", mock_feedparser_parse)
    
    retriever = ArxivRetriever(config)
    papers = retriever.retrieve_papers()
    parsed_results = [i for i in parsed_result.entries if i.get("arxiv_announce_type","new") == 'new']
    assert len(papers) == len(parsed_results)
    paper_titles = [i.title for i in papers]
    parsed_titles = [i.title for i in parsed_results]
    assert set(paper_titles) == set(parsed_titles)


def test_retrieve_papers_skips_failed_conversion(config, monkeypatch):
    retriever = ArxivRetriever(config)
    raw_papers = [
        ArxivPaperItem(
            entry_id="ok",
            title="ok",
            authors=["Alice"],
            summary="summary",
            pdf_url=None,
            source_url=None,
        ),
        ArxivPaperItem(
            entry_id="bad",
            title="bad",
            authors=["Bob"],
            summary="summary",
            pdf_url=None,
            source_url=None,
        ),
    ]

    monkeypatch.setattr(retriever, "_retrieve_raw_papers", lambda: raw_papers)
    original_convert_to_paper = retriever.convert_to_paper

    def mock_convert_to_paper(raw_paper):
        if raw_paper.entry_id == "bad":
            raise RuntimeError("boom")
        return original_convert_to_paper(raw_paper)

    monkeypatch.setattr(retriever, "convert_to_paper", mock_convert_to_paper)

    papers = retriever.retrieve_papers()

    assert len(papers) == 1
    assert papers[0].url == "ok"


def test_arxiv_paper_item_is_pickle_safe():
    item = ArxivPaperItem(
        entry_id="1234.5678",
        title="title",
        authors=["Alice"],
        summary="summary",
        pdf_url="https://arxiv.org/pdf/1234.5678",
        source_url="https://arxiv.org/e-print/1234.5678",
    )

    restored = pickle.loads(pickle.dumps(item))

    assert restored == item


def test_extract_markdown_from_pdf_with_timeout(tmp_path, monkeypatch):
    pdf_path = tmp_path / "paper.pdf"
    output_path = tmp_path / "paper.md"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    monkeypatch.setattr(
        "zotero_arxiv_daily.utils.PDF_EXTRACT_SCRIPT",
        "import time, sys\ntime.sleep(1)\n",
    )

    result = extract_markdown_from_pdf_with_timeout(str(pdf_path), timeout=0, output_path=str(output_path))

    assert result is None
    assert not Path(output_path).exists()
