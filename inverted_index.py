import json
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Generator

import platformdirs
import psutil

from JSONtokenizer import compute_word_frequencies, tokenize_JSON_file


@dataclass
class Posting:
    """
    Container for an inverted index document posting.
    """
    doc_id: int
    frequency: int

# The name of the entire A3 application.
_APP_NAME = 'CS121_A3'
# Local data dir for this application.
_APP_DATA_DIR = Path(platformdirs.user_data_dir(_APP_NAME))
_INDEXES_DIR = _APP_DATA_DIR / 'indexes'

# Percentage of remaining memory at which time the index should be flushed to disk.
_FLUSH_MEMORY_THRESHOLD = 0.5

# Wipe the old indexes, if any.
if _INDEXES_DIR.exists():
    shutil.rmtree(_INDEXES_DIR)

def _memory_low() -> bool:
    """
    Check if memory is too low.
    Returns:
        True if memory is too low, False otherwise.
    """
    # RAM & other virtual mem.
    vmem = psutil.virtual_memory()
    return vmem.percent < _FLUSH_MEMORY_THRESHOLD * 100

def _set_memory_low_th(perc: float):
    """
    Set the global low memory threshold.

    Args:
        perc: Percentage of total memory that is considered low memory.
    """
    if perc > 1 or perc < 0:
        raise ValueError('Percentage must be between 0 and 1.')

    global _FLUSH_MEMORY_THRESHOLD
    _FLUSH_MEMORY_THRESHOLD = perc

class InvertedIndex:
    """
    An inverted index storing document Posting objects by token.
    """
    def __init__(self):
        self._internal: dict[str, list[Posting]] = defaultdict(list)
        self._disk_dir: Path = _INDEXES_DIR / f'index-{self.__hash__()}'

    @property
    def disk_dir(self) -> Path:
        """
        The directory containing this index's partitioned disk files.

        Returns:
            Path to disk directory.
        """
        return self._disk_dir

    @property
    def disks(self) -> Generator[Path, None, None]:
        """
        The index's partitioned disk files. This is where index content is stored, which may be
        spread over multiple files.

        Returns:
            Generator of Paths pointing to disk files.
        """
        yield from self._disk_dir.iterdir()

    def add(self, json_path: Path):
        """
        Assimilate a document into the inverted index.

        Args:
            json_path: Path to JSON document.
        """
        for token, freq in compute_word_frequencies(tokenize_JSON_file(json_path)).items():
            # TODO: replace with real id once available.
            self._internal[token].append(Posting(doc_id = 0, frequency = freq))

        if _memory_low():
            self.flush()

    def flush(self):
        """
        Write the inverted index to disk and wipe its in-memory data. The location of the data
        is somewhere in the user data i.e. C:/../AppData/Local/... in windows. This path is
        accessible via the #disk_dir property.
        """
        path = self._disk_dir

        path.mkdir(parents = True, exist_ok = True)
        with open(path / 'disk.json', "w") as f:
            json.dump(self._internal, f, default = lambda o: o.__dict__)

        self._internal = defaultdict(list)

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
