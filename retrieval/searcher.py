from pathlib import Path

from index import InvertedIndex


class Searcher:
    """
    Wrapper around an inverted index used to retrieve relevant documents given search results.

    Args:
        source_dir_path: Path to the index/searcher data source directory.
    """
    def __init__(self, source_dir_path: str | Path):
        # The inverted index providing access to the data source.
        self._index = InvertedIndex(source_dir_path)

    def search(self, query: str) -> list[str]:
        """
        Retrieve the most relevant documents for a given query.

        Args:
            query: String search query.

        Returns:
            List of page urls ordered by relevance.
        """
        # TODO: Implement (#17)
        return list(f'https://www.example{count}.com' for count in range(100))