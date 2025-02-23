import json
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import platformdirs
import psutil

from index.JSONtokenizer import compute_word_frequencies, tokenize_JSON_file
from index.url_mapper import URLMapper


# The name of the entire A3 application.
_APP_NAME = 'CS121_A3'
# Local data dir for this application.
_APP_DATA_DIR = Path(platformdirs.user_data_dir(_APP_NAME))
_INDEXES_DIR = _APP_DATA_DIR / 'indexes'

# Default percentage of remaining memory at which time the index should be flushed to disk.
_DEF_FLUSH_MEMORY_THRESHOLD = 0.5
# Default number of postings in memory before the index should be flushed.
_DEF_FLUSH_POSTINGS_THRESHOLD = 10 ** 6

# Wipe the old indexes, if any.
if _INDEXES_DIR.exists():
    shutil.rmtree(_INDEXES_DIR)

@dataclass
class Posting:
    """
    Container for an inverted index document posting.
    """
    doc_id: int # ID of the document for this posting.
    frequency: int # Number of occurrences of this token in this document.

class _Partition:
    """
    A section (partition) of an inverted index. Essentially a sub inverted-index that represents
    a fragment of a larger inverted index. Allows for minimal I/O operations by segregating
    tokens into small indexes by lexicographical value. To find postings for the token 'cab', for
    example, instead of reading the entire inverted index from disk using multiple I/O operations
    (it's too large to load in one op) looking for 'cab', you'd only have to fetch the partition
    that handles tokens from 'ant' to 'crab' ('cab' falls in this range) which will be a file that
    can scanned in one I/O operation. Note the actual token range that a partition governs over may
    differ from the example.
    """
    pass

class InvertedIndex2:
    """
    An inverted index storing document Posting objects by token.

    Args:
        _id: Custom unique id. The default is to use the object's hash. This doubles as the name
        of the index dir.
        max_in_memory_postings: Maximum number of postings to store in memory. When this is crossed,
        the index is flushed to disk.
        min_avail_memory_perc: Minimum available memory as a percentage of the total available
        memory until the index is flushed to disk.
    """
    def __init__(self, root_dir: str | Path,
                 _id: str | int | None = None, *,
                 max_in_memory_postings: int = _DEF_FLUSH_POSTINGS_THRESHOLD,
                 min_avail_memory_perc: float = _DEF_FLUSH_MEMORY_THRESHOLD):
        self.id = str(_id) if _id else str(self.__hash__())
        self.max_in_memory_postings = max_in_memory_postings
        self.min_avail_memory_perc = min_avail_memory_perc

        # Directory that will contain all partitions for this index.
        self._partition_dir: Path = _INDEXES_DIR / f'index-{self.id}'
        self._temp_file = self._partition_dir / 'temp.json'

        # Total count of postings in virtual memory across all partitions.
        self._curr_in_memory_postings: int = 0

        # The partitions - "sub indexes" or "index sections". See the documentation for the
        # _Partition class above.
        self._partitions: list[_Partition] = list()

        self._in_memory: defaultdict[str, list[Posting]] = defaultdict(list)
        self._url_id_mapper = URLMapper(root_dir)

        self._feedr(root_dir)
        self._flush()

    def _add(self, json_path: Path):
        """
        Assimilate a document into the inverted index.

        Args:
            json_path: Path to JSON document.
        """
        for token, freq in compute_word_frequencies(tokenize_JSON_file(json_path)).items():
            # TODO: replace with real id once available.
            self._in_memory[token].append(Posting(doc_id = 0, frequency = freq))
            # Increment in-memory postings counter.
            self._curr_in_memory_postings += 1

            # If the in-memory index has become too large (too many postings or too little memory
            # available), flush (write) the index to physical disk file.
            if self._curr_in_memory_postings >= self.max_in_memory_postings or self._memory_low():
                self._flush()

    def _feedr(self, root_dir_path: str | Path):
        """
        Feed a directory of documents into the inverted index, recursively searching for and adding
        documents within the root directory and its child directories.

        Args:
            root_dir_path: Pathlike to the root directory to be fed.
        """
        # Get all nested jsons in the root dir and add it to the index.
        for file in Path(root_dir_path).rglob('*.json'):
            self._add(file)

    def _flush(self):
        """
        Write the inverted index to disk and wipe its in-memory data. The location of the data
        is somewhere in the user data i.e. C:/../AppData/Local/... in windows. This path is
        accessible via the #disk_dir property.
        """
        self._temp_file.parent.mkdir(parents = True, exist_ok = True)  # Ensure directory exists
        temp_merged_file = self._partition_dir / 'temp_merged.json'

        with open(temp_merged_file, 'w') as out:
            if self._temp_file.exists():
                with open(self._temp_file, 'r') as disk_file:
                    for line in disk_file:
                        entry = json.loads(line.strip())
                        token, postings = list(entry.items())[0]

                        if token in self._in_memory:
                            postings.extend(
                                posting.__dict__ for posting in self._in_memory.pop(token))

                        out.write(json.dumps({token: postings}) + '\n')

            for token, postings in self._in_memory.items():
                out.write(json.dumps({token: [posting.__dict__ for posting in postings]}) + '\n')

        temp_merged_file.replace(self._temp_file)

        self._in_memory.clear()
        self._curr_in_memory_postings = 0

    def _memory_low(self) -> bool:
        """
        Check if memory is too low.
        Returns:
            True if memory is too low, False otherwise.
        """
        # RAM & other virtual mem.
        vmem = psutil.virtual_memory()
        return vmem.percent < self.min_avail_memory_perc * 100