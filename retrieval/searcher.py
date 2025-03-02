from pathlib import Path

from index.inverted_index import InvertedIndex, Posting
from index.path_mapper import PathMapper

class Searcher:
    """
    Wrapper around an inverted index used to retrieve relevant documents given search results.

    Args:
        source_dir_path: Path to the index/searcher data source directory.
    """
    def __init__(self, source_dir_path: str | Path):
        # The inverted index providing access to the data source.
        self._index = InvertedIndex(source_dir_path)
        self.path_mapper = PathMapper(source_dir_path)

    def search(self, query: str) -> list[str]:
        """
        Retrieve the most relevant documents for a given query.

        Args:
            query: String search query.

        Returns:
            List of page urls ordered by relevance.
        """
        
        query_tokens = query.lower().split()
        doc_scores = {}
        
        for token in query_tokens:
            token_docs = self._index[token]

            for posting in token_docs:
                doc_id = posting.doc_id
                count = posting.frequency

                if doc_id in doc_scores:
                    doc_scores[doc_id] += count
                else:
                    doc_scores[doc_id] = count
        
        # filter documents to only include all query tokens
        filtered_docs = {
            doc : score 
            for doc, score in doc_scores.items()
            if all(any(p.doc_id == doc for p in self._index[token])
            for token in query_tokens)
        }

        # sort documents by relevance score
        sorted_docs = sorted(filtered_docs.items(), key = lambda x : x[1], reverse = True)

        # get the urls based on document id
        result_urls = [
            self.path_mapper.get_url_by_id(doc_id) 
            for doc_id, _ in sorted_docs 
            if self.path_mapper.get_url_by_id(doc_id)
        ]
        
        return result_urls


