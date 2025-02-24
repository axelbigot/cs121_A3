import json
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import platformdirs
import psutil

from JSONtokenizer import compute_word_frequencies, tokenize_JSON_file


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
    def __init__(self, partition_dir: Path):
        # The in-memory inverted index (for this partition).
        # This is the portion of the partition that is "being built" and has not yet been written
        # to disk.
        self._in_memory: dict[str, list[Posting]] = defaultdict(list)

        # The root directory for this partition. This is where other partitions belonging to the
        # same full inverted index will also be stored.
        self._partition_dir: Path = partition_dir

        # The smallest lexicographical token handled by this partition. The lower limit of contained
        # tokens (inclusive).
        self._min_token: str | None = None

        # The greatest lexicographical token handled by this partition. The upper limit of contained
        # tokens (inclusive).
        self._max_token: str | None = None

        # The path to this partitions disk storage.
        self._path: Path | None = None

        # Whether this partition has written to disk.
        self._has_disk = False

    @property
    def min_token(self) -> str | None:
        """
        The smallest lexicographical token handled by this partition. The lower limit of contained
        tokens (inclusive). If a token is >= min_token and <= max_token, it is guaranteed to be
        contained within this partition, if it exists anywhere in the inverted index.

        Returns:
            Smallest-order string token of this partition.
        """
        return self._min_token

    @property
    def max_token(self) -> str | None:
        """
        The greatest lexicographical token handled by this partition. The upper limit of contained
        tokens (inclusive). If a token is >= min_token and <= max_token, it is guaranteed to be
        contained within this partition, if it exists anywhere in the inverted index.

        Returns:
            Largest-order string token of this partition.
        """
        return self._max_token

    @property
    def path(self) -> Path | None:
        """
        The path to this partitions disk storage.

        Returns:
            Path object to this partition's physical disk JSON file.
        """
        return self._path

    def has_disk(self) -> bool:
        """
        Whether this partition has a physical disk component and has actually written to disk.

        Returns:
            True if a write to disk occurred, False otherwise.
        """
        return self._has_disk

    def has_memory(self) -> bool:
        """
        Whether this partition has a virtual memory component (some portion of the partition is not
        written to disk, perhaps it is pending a flush).

        Returns:
            True if partition contains unflushed data, False otherwise.
        """
        return bool(self._in_memory)

    def add(self, token: str, posting: Posting):
        """
        Add a posting to this partition.

        Args:
            token: The token associated with this posting.
            posting: The posting object.
        """
        self._in_memory[token].append(posting)

    def get(self, token: str) -> list[Posting]:
        """
        Retrieve a token's postings from this partition. Searches both virtual and physical (disk)
        components of the partition.

        Args:
            token: The token whose associated postings to retrieve.

        Returns:
            List of Posting objects associated with the token.
        """
        # Start by retrieving any token postings in the virtual memory component.
        all_data = self._in_memory[token]

        # If a physical component exists, fetch the token postings and combine them with the
        # virtual postings.
        if self.has_disk():
            all_data += self.fetch()[token]

        # The final result is all the token's postings, be they written to disk or not.
        return all_data

    def fetch(self) -> defaultdict[str, list[Posting]]:
        """
        Fetch this posting's physical (disk) component and convert it to virtual memory.

        Returns:
            Dict with string tokens as keys and associated Posting objects as values.
        """
        # Unmarshall the JSON on the physical disk. We now have a raw python object.
        with open(self._path, 'r') as f:
            raw = json.load(f)

        # Massage the raw object into our custom types to make it easier to work with.
        result = defaultdict(list)
        for token, raw_postings in raw.items():
            result[token] = [Posting(**p) for p in raw_postings]

        # The end result is the physical inverted index for this partition in the same format as
        # the in-memory component.
        return result

    def flush(self):
        """
        Flush this partition's virtual inverted index to disk, merging with existing physical
        component if any exists.
        """
        # TODO: Split physical partition if too large. Currently getting very skewed partitions!

        # If this partition has never written to disk, initialize its defining attributes.
        if not self.has_disk():
            sorted_tokens = sorted(self._in_memory)
            # Set the min and max tokens of the partition. This is the "scope" of the partition.
            self._min_token, self._max_token = sorted_tokens[0], sorted_tokens[-1]
            # Set the path to its physical disk, which will be created shortly.
            self._path = self._partition_dir / f'partition_{self._min_token}_{self._max_token}'

        # Create disk if DNE.
        self._path.parent.mkdir(parents = True, exist_ok = True)

        # Merge current disk (if any) with virtual component (in-place merging).
        new_disk = self._merge_with_disk() if self.has_disk() else self._in_memory
        with open(self._path, "w") as f:
            # Write to disk
            json.dump(new_disk, f, default = lambda o: o.__dict__)

        # Update flag after successful flush.
        self._has_disk = True
        # Clear virtual memory. It's all written to disk now.
        self._in_memory.clear()

    def items(self) -> Generator[tuple[str, list[Posting]], None, None]:
        """
        Get the token-postings items of this partition, searching both physical and virtual
        components.

        Returns:
            Generator of dictionary-style token-postings items.
        """
        yield from self._merge_with_disk().items() if self.has_disk() else self._in_memory.items()

    def _merge_with_disk(self) -> dict[str, list[Posting]]:
        """
        Merge this partition's virtual inverted index with existing disk component if any exists.
        Retrieves the physical inverted index from disk and transfers postings from the virtual
        component to it. Given the physical component size P and virtual component size V, the
        worst-case space complexity of this operation is T(P + V + 1) (const 1 posting during
        transfer).

        Returns:
            Merged physical and virtual inverted index components, now all in memory.
        """
        # Retrieve physical component.
        disk = self.fetch()
        for token in list(self._in_memory.keys()):
            if token in disk:
                # Append token postings from memory component to existing entries on disk.
                disk[token].extend(self._in_memory[token])
            else:
                # No entries for this token on disk (new from virtual).
                disk[token] = self._in_memory[token]

            # Delete transferred postings from memory, ensuring minimal memory usage.
            del self._in_memory[token]
        return disk

    def __iter__(self) -> Generator[str, None, None]:
        """
        Iterate over all tokens in this partition, including both physical and virtual components.

        Returns:
            Generator of token strings.
        """
        yield from self._merge_with_disk() if self.has_disk() else self._in_memory

    def __str__(self):
        return json.dumps(self._in_memory, default = lambda o: o.__dict__, indent = 2)

    def __repr__(self):
        return f'Partition(min_token = {self._min_token}, max_token = {self._max_token})'

class InvertedIndex:
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
    def __init__(self, _id: str | int | None = None, *,
                 max_in_memory_postings: int = _DEF_FLUSH_POSTINGS_THRESHOLD,
                 min_avail_memory_perc: float = _DEF_FLUSH_MEMORY_THRESHOLD):
        self.id = str(_id) if _id else str(self.__hash__())
        self.max_in_memory_postings = max_in_memory_postings
        self.min_avail_memory_perc = min_avail_memory_perc
        self.doc_count = 0

        # Directory that will contain all partitions for this index.
        self._partition_dir: Path = _INDEXES_DIR / f'index-{self.id}'

        # Total count of postings in virtual memory across all partitions.
        self._curr_in_memory_postings: int = 0

        # The partitions - "sub indexes" or "index sections". See the documentation for the
        # _Partition class above.
        self._partitions: list[_Partition] = list()

        # Special memory-only partition used for postings that do not have an existing partition.
        # They are all added here, and flushed for the first time in the next flush cycle, at which
        # point this special partition is reset to an empty partition for the next cycle.
        self._misc_partition: _Partition = _Partition(self._partition_dir)

    @property
    def partition_dir(self) -> Path:
        """
        The directory containing this index's partitioned disk files.

        Returns:
            Path to disk directory.
        """
        return self._partition_dir

    @property
    def disk_partitions(self) -> Generator[Path, None, None]:
        """
        The index's partitioned disk files. These are the physical indexes where content is stored,
        which may be spread over multiple files.

        Returns:
            Generator of Paths pointing to disk files.
        """
        return (p.path for p in self._partitions)

    def add(self, json_path: Path):
        """
        Assimilate a document into the inverted index.

        Args:
            json_path: Path to JSON document.
        """
        for token, freq in compute_word_frequencies(tokenize_JSON_file(json_path)).items():
            # TODO: replace with real id once available.
            # Add the partition for this token to the correct partition.
            self._get_partition(token).add(token, Posting(doc_id = 0, frequency = freq))
            # Increment in-memory postings counter.
            self._curr_in_memory_postings += 1

            # If the in-memory index has become too large (too many postings or too little memory
            # available), flush (write) the index to physical disk file.
            if self._curr_in_memory_postings >= self.max_in_memory_postings or self._memory_low():
                self.flush()
        self.doc_count += 1

    def feedr(self, root_dir_path: str | Path):
        """
        Feed a directory of documents into the inverted index, recursively searching for and adding
        documents within the root directory and its child directories.

        Args:
            root_dir_path: Pathlike to the root directory to be fed.
        """
        # Get all nested jsons in the root dir and add it to the index.
        for file in Path(root_dir_path).rglob('*.json'):
            self.add(file)

    def flush(self):
        """
        Write the inverted index to disk and wipe its in-memory data. The location of the data
        is somewhere in the user data i.e. C:/../AppData/Local/... in windows. This path is
        accessible via the #disk_dir property.
        """
        # Flush each partition to disk.
        for partition in self._partitions:
            partition.flush()

        # Flush the purely virtual miscellaneous partition to disk IFF it has content.
        if self._misc_partition.has_memory():
            self._misc_partition.flush()
            # Flushed, it is now a physical partition. Add it to the list of partitions.
            self._partitions.append(self._misc_partition)

        # Reset for the next cycle.
        self._curr_in_memory_postings = 0
        self._misc_partition = _Partition(self._partition_dir)

    def items(self) -> Generator[tuple[str, list[Posting]], None, None]:
        """
        Get the token-postings items of this index, searching both physical and virtual components
        of all partitions.

        Returns:
            Generator of dictionary-style token-postings items.
        """
        for partition in self._partitions + [self._misc_partition]:
            yield from partition.items()

    def _get_partition(self, token: str) -> _Partition:
        """
        Get the partition that governs a specified token.

        Args:
            token: The string token.

        Returns:
            Partition object that the token is or will be a part of.
        """
        for partition in self._partitions:
            # Find the partition whose 'jurisdiction' this token falls under based on its
            # lexicographical order.
            if partition.min_token <= token <= partition.max_token:
                return partition

        # If no existing partitions govern the token, add it to the special partition.
        return self._misc_partition

    def _memory_low(self) -> bool:
        """
        Check if memory is too low.
        Returns:
            True if memory is too low, False otherwise.
        """
        # RAM & other virtual mem.
        vmem = psutil.virtual_memory()
        return vmem.percent < self.min_avail_memory_perc * 100

    def __getitem__(self, token: str) -> list[Posting]:
        """
        Retrieve all Postings for a given token.

        Args:
            token: The token whose Postings to retrieve.

        Returns:
            A list of all Postings for a given token.
        """
        return self._get_partition(token).get(token)

    def __iter__(self) -> Generator[str, None, None]:
        """
        Iterate over all tokens across all partitions. Looks for tokens in disk and in memory while
        respecting memory limits (only one partition is ever loaded at a time).

        Returns:
            Generator of token strings.
        """
        for partition in self._partitions + [self._misc_partition]:
            # Partitions are iterable. They return all of their token-postings dict items.
            yield from partition

    def __str__(self) -> str:
        return ' '.join(map(lambda p: str(p), self._partitions))

    def __repr__(self):
        return (f'InvertedIndex(max_in_memory_postings = {self.max_in_memory_postings}, '
                f'min_avail_memory_perc = {self.min_avail_memory_perc}, '
                f'partitions = {self._partitions.__repr__()})')
