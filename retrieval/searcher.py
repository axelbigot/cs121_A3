from collections import defaultdict
from pathlib import Path
import math
import logging
import json
import re

from nltk.stem import PorterStemmer
from spellchecker import SpellChecker
from textblob import Word
import time

from index.inverted_index import InvertedIndex, Posting
from index.path_mapper import PathMapper
from index.JSONtokenizer import compute_word_frequencies, tokenize, get_soup_from_JSON
from index.defs import APP_DATA_DIR

_SEARCHER_DIR = APP_DATA_DIR / 'searcher'
logger = logging.getLogger(__name__)

HTML_TAGS_WEIGHTS = {
    "h1": 0.2, "h2": 0.15, "h3": 0.1, "title": 0.4, "b": 0.075, "strong": 0.055, "other": 0.02
}

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
        self.spellchecker = SpellChecker()
        self._document_vectors = dict()

        name = re.sub(r'[<>:"/\\|?*]', '_', source_dir_path)
        self._searcher_disk_path = _SEARCHER_DIR / f'{name}.json'

        if not kwargs['load_existing'] or not self._load():
            print("Building document_vectors from scratch")
            logger.debug(f'Building document_vectors from scratch')

            for doc_id, path in self.path_mapper.id_to_path.items():
                soup = get_soup_from_JSON(path)
                self._document_vectors[doc_id] = compute_word_frequencies(tokenize(soup.get_text(' ')))

            self._save()

        Word("octopi").lemmatize() # used to load data from textblob library so first 
                                       # query processing time is the same as the others

    def _save(self):
        """
        Saves the document vectors to disk, so it does not need to be recalculated for later runs.
        """
        data = {
            'document_vectors': self._document_vectors
        }

        self._searcher_disk_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self._searcher_disk_path, 'w') as f:
            json.dump(data, f)

        logger.debug(f'Saved searcher to {self._searcher_disk_path}')

    def _load(self) -> bool:
        """
        Load the document vectors from disk, if it already exists.

        Returns:
            True if loading succeeded, false otherwise.
        """
        if not self._searcher_disk_path.exists():
            logger.debug(f'Count not find existing Searcher')
            return False

        logger.debug(f'Loading Searcher from {self._searcher_disk_path}')

        with open(self._searcher_disk_path, 'r') as f:
            data = json.load(f)

        self._document_vectors = data["document_vectors"]

        return True

    def _process_query(self, query: str) -> set[str]:
        """
        Processes the query by normalizing, tokenizing, lemmatizing, expanding terms,
        and correcting spelling errors.

        Expands terms using synonyms from WordNet and stemming.

        Args:
            query: String search query.

        Returns:
            A set of processed query tokens.
        """

        # Normalize and tokenize
        tokens = query.lower().split()

        # Correct spelling errors
        corrected_tokens = {self.spellchecker.correction(token) or token for token in tokens}

        # Lemmatize tokens
        lemmatized_tokens = {Word(token).lemmatize() for token in corrected_tokens}

        # Combine all variations
        expanded_tokens = lemmatized_tokens.union(corrected_tokens)
        return expanded_tokens

    def _cosine_similarity(self, query: str, doc_id: str):
        """
        Calculate the cosine similarity between a query and a document. The document
        should not include the HTML tags (i.e use soup.get_text()). 1 is similar, 0 is completely different

        Args:
            query: search query
            doc_id: id of the document according to self.path_mapper

        Returns:
            A number representing the cosine of the angle between the 2 strings in vector space.
        """
        # calculate vectors for both strings (vector = word frequencies)
        query_vector = compute_word_frequencies(tokenize(query))
        document_vector = self._document_vectors[doc_id]

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
            Search time of query
        """

        start_time = time.perf_counter()

        query_tokens = self._process_query(query)
        doc_scores: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        for token in query_tokens:
            token_docs = self._index[token].postings
            token_df = self._index[token].df

            for posting in token_docs:
                doc_id = posting.doc_id
                idf = 0 if token_df == 0 or self._index.page_count == 0 else math.log(self._index.page_count / token_df)

                tfidf = 0
                for tag, frequency in posting.tag_frequencies.items():
                    log = 0 if frequency == 0 else math.log(frequency)
                    tfidf += HTML_TAGS_WEIGHTS[tag] * (1 + log) * idf

                doc_scores[doc_id][token] += tfidf

        # filter documents to only include all query tokens
        filtered_docs = {doc: sum(entries.values())
                         for doc, entries in doc_scores.items() if entries.keys() == query_tokens}

        # sort documents by relevance score
        sorted_docs = sorted(filtered_docs.items(), key = lambda x : x[1], reverse = True)

        # using cosign similarity on sorted items
        cosign_scores: dict[int, int] = defaultdict(int)
        for document_id, _ in sorted_docs[:50]:
            cosign_scores[document_id] = self._cosine_similarity(query, str(document_id))

        # resorting
        sorted_docs = sorted(cosign_scores.items(), key = lambda x : x[1], reverse = True)

        # get the urls based on document id
        result_urls = [
            self.path_mapper.get_url_by_id(doc_id) 
            for doc_id, _ in sorted_docs 
            if self.path_mapper.get_url_by_id(doc_id)
        ]

        end_time = time.perf_counter()
        search_time = f"Found {len(result_urls)} results in {round(end_time - start_time, 3)} seconds"

        return result_urls, search_time

