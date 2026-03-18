from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from omegaconf import DictConfig
from loguru import logger
from tqdm import tqdm
from typing import Generic, Type

from ..protocol import Paper, RawPaperItem


class BaseRetriever(ABC, Generic[RawPaperItem]):
    name: str
    def __init__(self, config:DictConfig):
        self.config = config
        self.retriever_config = getattr(config.source,self.name)

    @abstractmethod
    def _retrieve_raw_papers(self) -> list[RawPaperItem]:
        pass

    @abstractmethod
    def convert_to_paper(self, raw_paper:RawPaperItem) -> Paper | None:
        pass

    def safe_convert_to_paper(self, raw_paper: RawPaperItem) -> Paper | None:
        try:
            return self.convert_to_paper(raw_paper)
        except Exception as e:
            logger.warning(f"Failed to convert paper {raw_paper}: {e}")
            return None

    def retrieve_papers(self) -> list[Paper]:
        raw_papers = self._retrieve_raw_papers()
        logger.info("Processing papers...")
        with ThreadPoolExecutor(max_workers=self.config.executor.max_workers) as exec_pool:
            futures = {exec_pool.submit(self.safe_convert_to_paper, rp): i for i, rp in enumerate(raw_papers)}
            papers: list[Paper | None] = [None] * len(raw_papers)
            for future in tqdm(as_completed(futures), total=len(raw_papers), desc="Converting papers"):
                try:
                    papers[futures[future]] = future.result()
                except Exception as e:
                    logger.warning(f"Paper conversion future failed: {e}")
        return [p for p in papers if p is not None]

registered_retrievers = {}

def register_retriever(name:str):
    def decorator(cls):
        registered_retrievers[name] = cls
        cls.name = name
        return cls
    return decorator

def get_retriever_cls(name:str) -> Type[BaseRetriever]:
    if name not in registered_retrievers:
        raise ValueError(f"Retriever {name} not found")
    return registered_retrievers[name]
