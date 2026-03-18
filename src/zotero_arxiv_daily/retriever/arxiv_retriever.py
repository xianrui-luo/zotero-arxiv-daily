from .base import BaseRetriever, register_retriever
import arxiv
from arxiv import Result as ArxivResult
from dataclasses import dataclass
from ..protocol import Paper
from ..utils import extract_markdown_from_pdf_with_timeout, extract_tex_code_from_tar
from tempfile import TemporaryDirectory
import feedparser
from urllib.request import urlretrieve
from tqdm import tqdm
import os
from loguru import logger

PDF_EXTRACT_TIMEOUT = 180


@dataclass
class ArxivPaperItem:
    entry_id: str
    title: str
    authors: list[str]
    summary: str
    pdf_url: str | None
    source_url: str | None


@register_retriever("arxiv")
class ArxivRetriever(BaseRetriever[ArxivPaperItem]):
    def __init__(self, config):
        super().__init__(config)
        if self.config.source.arxiv.category is None:
            raise ValueError("category must be specified for arxiv.")
    def _retrieve_raw_papers(self) -> list[ArxivPaperItem]:
        client = arxiv.Client(num_retries=10,delay_seconds=10)
        query = '+'.join(self.config.source.arxiv.category)
        include_cross_list = self.config.source.arxiv.get("include_cross_list", False)
        # Get the latest paper from arxiv rss feed
        feed = feedparser.parse(f"https://rss.arxiv.org/atom/{query}")
        if 'Feed error for query' in feed.feed.title:
            raise Exception(f"Invalid ARXIV_QUERY: {query}.")
        raw_papers = []
        allowed_announce_types = {"new", "cross"} if include_cross_list else {"new"}
        all_paper_ids = [
            i.id.removeprefix("oai:arXiv.org:")
            for i in feed.entries
            if i.get("arxiv_announce_type", "new") in allowed_announce_types
        ]
        if self.config.executor.debug:
            all_paper_ids = all_paper_ids[:10]

        # Get full information of each paper from arxiv api
        bar = tqdm(total=len(all_paper_ids))
        for i in range(0,len(all_paper_ids),20):
            search = arxiv.Search(id_list=all_paper_ids[i:i+20])
            batch = [normalize_arxiv_result(result) for result in client.results(search)]
            bar.update(len(batch))
            raw_papers.extend(batch)
        bar.close()

        return raw_papers

    def convert_to_paper(self, raw_paper:ArxivPaperItem) -> Paper:
        title = raw_paper.title
        authors = raw_paper.authors
        abstract = raw_paper.summary
        pdf_url = raw_paper.pdf_url
        full_text = extract_text_from_pdf(raw_paper)
        if full_text is None:
            full_text = extract_text_from_tar(raw_paper)
        return Paper(
            source=self.name,
            title=title,
            authors=authors,
            abstract=abstract,
            url=raw_paper.entry_id,
            pdf_url=pdf_url,
            full_text=full_text
        )


def normalize_arxiv_result(result: ArxivResult) -> ArxivPaperItem:
    return ArxivPaperItem(
        entry_id=result.entry_id,
        title=result.title,
        authors=[author.name for author in result.authors],
        summary=result.summary,
        pdf_url=result.pdf_url,
        source_url=result.source_url(),
    )


def extract_text_from_pdf(paper: ArxivPaperItem) -> str | None:
    with TemporaryDirectory() as temp_dir:
        pdf_path = os.path.join(temp_dir, "paper.pdf")
        markdown_path = os.path.join(temp_dir, "paper.md")
        if paper.pdf_url is None:
            logger.warning(f"No PDF URL available for {paper.title}")
            return None
        try:
            urlretrieve(paper.pdf_url, pdf_path)
        except Exception as e:
            logger.warning(f"Failed to download pdf of {paper.title}: {e}")
            return None

        full_text = extract_markdown_from_pdf_with_timeout(pdf_path, PDF_EXTRACT_TIMEOUT, markdown_path)
        if full_text is None:
            logger.warning(f"Failed to extract full text of {paper.title} from pdf")
        return full_text


def extract_text_from_tar(paper: ArxivPaperItem) -> str | None:
    with TemporaryDirectory() as temp_dir:
        path = os.path.join(temp_dir, "paper.tar.gz")
        source_url = paper.source_url
        if source_url is None:
            logger.warning(f"No source URL available for {paper.title}")
            return None
        try:
            urlretrieve(source_url, path)
        except Exception as e:
            logger.warning(f"Failed to download source of {paper.title}: {e}")
            return None

        try:
            file_contents = extract_tex_code_from_tar(path, paper.entry_id)
            if file_contents is None or "all" not in file_contents:
                logger.warning(f"Failed to extract full text of {paper.title} from tar: Main tex file not found.")
                return None
            full_text = file_contents["all"]
        except Exception as e:
            logger.warning(f"Failed to extract full text of {paper.title} from tar: {e}")
            full_text = None
        return full_text
