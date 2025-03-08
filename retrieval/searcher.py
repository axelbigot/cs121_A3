from collections import defaultdict
from pathlib import Path
import math

from index.inverted_index import InvertedIndex, Posting
from index.path_mapper import PathMapper
from index.JSONtokenizer import compute_word_frequencies, tokenize, get_content_from_JSON

class Searcher:
    """
    Wrapper around an inverted index used to retrieve relevant documents given search results.

    Args:
        source_dir_path: Path to the index/searcher data source directory.
    """
    def __init__(self, source_dir_path: str | Path, **kwargs):
        # The inverted index providing access to the data source.
        self._index = InvertedIndex(source_dir_path, **kwargs)
        self.path_mapper = self._index._mapper

    def _cosine_similarity(self, query: str, document_text: str):
        """
        Calculate the cosine similarity between a query and a document. The document
        should not include the HTML tags (i.e use soup.get_text()). 1 is similar, 0 is completely different

        Args:
            query: search query
            document_text: text portion of a document

        Returns:
            A number representing the cosine of the angle between the 2 strings in vector space.
        """
        # calculate vectors for both strings (vector = word frequencies)
        query_vector = compute_word_frequencies(tokenize(query))
        document_vector = compute_word_frequencies(tokenize(document_text))
        
        # calculate cosine
        numerator = sum(query_vector.get(word, 0) * document_vector.get(word, 0) for word in set(list(query_vector.keys()) + list(document_vector.keys())))
        denominator = math.sqrt(sum(frequency ** 2 for frequency in query_vector.values()) * sum(frequency ** 2 for frequency in document_vector.values()))

        return numerator / denominator

    def search(self, query: str) -> list[str]:
        """
        Retrieve the most relevant documents for a given query.

        Args:
            query: String search query.

        Returns:
            List of page urls ordered by relevance.
        """
        
        query_tokens = set(query.lower().split())
        doc_scores: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        for token in query_tokens:
            token_docs = self._index[token].postings
            token_df = self._index[token].df

            for posting in token_docs:
                doc_id = posting.doc_id
                idf = 0 if token_df == 0 or self._index.page_count == 0 else math.log(self._index.page_count / token_df)

                tfidf = (1 + math.log(posting.frequency)) * idf
                doc_scores[doc_id][token] += tfidf

        # filter documents to only include all query tokens
        filtered_docs = {doc: sum(entries.values())
                         for doc, entries in doc_scores.items() if entries.keys() == query_tokens}

        # sort documents by relevance score
        sorted_docs = sorted(filtered_docs.items(), key = lambda x : x[1], reverse = True)

        # using cosign similarity on sorted items
        cosign_scores: dict[int, int] = defaultdict(int)
        for document_id, _ in sorted_docs[:50]:
            document_path = self.path_mapper.get_path_by_id(document_id)
            document_text = get_content_from_JSON(document_path)

            cosign_scores[document_id] = self._cosine_similarity(query, document_text)

        # resorting
        sorted_docs = sorted(cosign_scores.items(), key = lambda x : x[1], reverse = True)

        # get the urls based on document id
        result_urls = [
            self.path_mapper.get_url_by_id(doc_id) 
            for doc_id, _ in sorted_docs 
            if self.path_mapper.get_url_by_id(doc_id)
        ]
        
        return result_urls


