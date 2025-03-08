from collections import defaultdict
from pathlib import Path

from nltk.corpus import wordnet
from nltk.stem import PorterStemmer
from spellchecker import SpellChecker
from textblob import Word

from index.inverted_index import InvertedIndex, Posting
from index.path_mapper import PathMapper

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
        # Initialize spell checker
        spell = SpellChecker()
        
        # Normalize and tokenize
        tokens = query.lower().split()
        
        # Correct spelling errors
        corrected_tokens = {spell.correction(token) or token for token in tokens}
        
        # Lemmatize tokens
        lemmatized_tokens = {Word(token).lemmatize() for token in corrected_tokens}
        
        # Expand with synonyms from WordNet
        synonym_tokens = set()
        for token in lemmatized_tokens:
            for syn in wordnet.synsets(token):
                for lemma in syn.lemmas():
                    synonym_tokens.add(lemma.name().replace('_', ' '))  # Handle multi-word synonyms

        # Combine all variations
        expanded_tokens = lemmatized_tokens.union(synonym_tokens, corrected_tokens)

        return expanded_tokens

    def search(self, query: str) -> list[str]:
        """
        Retrieve the most relevant documents for a given query.

        Args:
            query: String search query.

        Returns:
            List of page urls ordered by relevance.
        """
        
        query_tokens = self._process_query(query)
        doc_scores: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        for token in query_tokens:
            token_docs = self._index[token]

            for posting in token_docs:
                doc_id = posting.doc_id
                count = posting.frequency

                doc_scores[doc_id][token] += count

        # filter documents to only include all query tokens
        filtered_docs = {doc: sum(entries.values())
                         for doc, entries in doc_scores.items() if entries.keys() == query_tokens}

        # sort documents by relevance score
        sorted_docs = sorted(filtered_docs.items(), key = lambda x : x[1], reverse = True)

        # get the urls based on document id
        result_urls = [
            self.path_mapper.get_url_by_id(doc_id) 
            for doc_id, _ in sorted_docs 
            if self.path_mapper.get_url_by_id(doc_id)
        ]
        
        return result_urls