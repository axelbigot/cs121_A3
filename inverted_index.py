import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from JSONtokenizer import compute_word_frequencies, tokenize_JSON_file


@dataclass
class Posting:
    """
    Container for an inverted index document posting.
    """
    doc_id: int
    frequency: int

class InvertedIndex:
    """
    An inverted index storing document Posting objects by token.
    """
    def __init__(self):
        self._internal: dict[str, list[Posting]] = defaultdict(list)

    def add(self, json_path: Path):
        """
        Assimilate a document into the inverted index.

        Args:
            json_path: Path to JSON document.
        """
        for token, freq in compute_word_frequencies(tokenize_JSON_file(json_path)).items():
            # TODO: replace with real id once available.
            self._internal[token].append(Posting(doc_id = 0, frequency = freq))

    def __getitem__(self, token: str) -> list[Posting]:
        """
        Retrieve all Postings for a given token.

        Args:
            token: The token whose Postings to retrieve.

        Returns:
            A list of all Postings for a given token.
        """
        return self._internal[token]

    def __iter__(self):
        """
        Iterate over all token-postings.

        Returns:
            Immutable iterable of token-postings dict.
        """
        return iter(MappingProxyType(self._internal))

    def __str__(self) -> str:
        """
        String representation of the index.

        Returns:
            A string representation of the index.
        """
        return json.dumps(self._internal, default = lambda o: o.__dict__, indent = 2)

def construct_index_recursively(root_dir_path: str) -> InvertedIndex:
    """
    Builds an inverted index from all json files in a directory, recursively adding all json
    documents to the index.

    Args:
        root_dir_path: String path to the root directory.

    Returns:
        An in-memory inverted index containing all json documents in the root directory.
    """
    index = InvertedIndex()
    for file in Path(root_dir_path).rglob('*.json'):
        index.add(file)

    return index
