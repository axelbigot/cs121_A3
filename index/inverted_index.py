import json
import shutil
from collections import defaultdict
from dataclasses import dataclass
from itertools import tee
from pathlib import Path
from typing import Generator

import platformdirs
import psutil
from typing_extensions import TextIO

from index.JSONtokenizer import compute_word_frequencies, tokenize_JSON_file
from index.path_mapper import PathMapper


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

    Args:
        partition_dir: Directory where all other related partitions are stored.
        postings: The inverted index postings that this partition will govern.
    """
    def __init__(self, partition_dir: Path, postings: dict[str, list[Posting]]):
        # The in-memory inverted index (for this partition).
        # This is the portion of the partition that is "being built" and has not yet been written
        # to disk.
        self._in_memory: dict[str, list[Posting]] = postings

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

        # Write to disk.
        self._flush()

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

    def get(self, token: str) -> list[Posting]:
        """
        Retrieve a token's postings from this partition. Searches both virtual and physical (disk)
        components of the partition.

        Args:
            token: The token whose associated postings to retrieve.

        Returns:
            List of Posting objects associated with the token.
        """
        return self.fetch()[token]

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

    def items(self) -> Generator[tuple[str, list[Posting]], None, None]:
        """
        Get the token-postings items of this partition, searching both physical and virtual
        components.

        Returns:
            Generator of dictionary-style token-postings items.
        """
        yield from self.fetch().items()

    def _flush(self):
        """
        Flush this partition's virtual inverted index to disk, merging with existing physical
        component if any exists.
        """
        # Initialize this partition's defining attributes.
        sorted_tokens = sorted(self._in_memory)
        # Set the min and max tokens of the partition. This is the "scope" of the partition.
        self._min_token, self._max_token = sorted_tokens[0], sorted_tokens[-1]
        # Set the path to its physical disk, which will be created shortly.
        self._path = self._partition_dir / f'partition_{self._min_token}_{self._max_token}.json'

        # Create disk if DNE.
        self._path.parent.mkdir(parents = True, exist_ok = True)

        with open(self._path, "w") as f:
            # Write to disk
            json.dump(self._in_memory, f, default = lambda o: o.__dict__)

        # Clear virtual memory. It's all written to disk now.
        self._in_memory.clear()

    def __iter__(self) -> Generator[str, None, None]:
        """
        Iterate over all tokens in this partition, including both physical and virtual components.

        Returns:
            Generator of token strings.
        """
        yield from self.fetch()

    def __str__(self):
        return json.dumps(self.fetch(), default = lambda o: o.__dict__, indent = 2)

    def __repr__(self):
        return f'Partition(min_token = {self._min_token}, max_token = {self._max_token})'

class InvertedIndex:
    """
    An inverted index storing document Posting objects by token.

    Args:
        root_dir: The root directory from where to being json file processing when building the
        index.
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
        self.doc_count = 0

        # Directory that will contain all partitions for this index.
        self._partition_dir: Path = _INDEXES_DIR / f'index-{self.id}'
        self._temp_file = self._partition_dir / 'temp.json'

        # Total count of postings in virtual memory across all partitions.
        self._curr_in_memory_postings: int = 0

        # The partitions - "sub indexes" or "index sections". See the documentation for the
        # _Partition class above.
        self._partitions: list[_Partition] = list()

        # In-memory portion of the index.
        self._in_memory: defaultdict[str, list[Posting]] = defaultdict(list)
        # URL-to-id mapper.
        self._url_id_mapper = PathMapper(root_dir)

        # TODO: Hacky way of representing a merged index as a single partition. In the future,
        # TODO: this will be replaced by dedicated partitions.
        self._min_token: str | None = None
        self._max_token: str | None = None

        self._feedr(root_dir)
        self._flush()

        # TODO: Part of same hack above. Will be removed when partitions are properly implemented.
        # TODO: for now, we're using a single merged index.
        self._partitions.append(_Partition(self._partition_dir,
                                           {self._min_token: [], self._max_token: []}))
        self._temp_file.replace(self._partitions[0].path)

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

    def items(self) -> Generator[tuple[str, list[Posting]], None, None]:
        """
        Get the token-postings items of this index, searching both physical and virtual components
        of all partitions.

        Returns:
            Generator of dictionary-style token-postings items.
        """
        for p in self._partitions:
            yield from p.items()

    def __getitem__(self, token: str) -> list[Posting]:
        """
        Retrieve all Postings for a given token.

        Args:
            token: The token whose Postings to retrieve.

        Returns:
            A list of all Postings for a given token.
        """
        partition = self._get_partition(token)
        return partition.get(token) if partition else []

    def __iter__(self) -> Generator[str, None, None]:
        """
        Iterate over all tokens across all partitions. Looks for tokens in disk and in memory while
        respecting memory limits (only one partition is ever loaded at a time).

        Returns:
            Generator of token strings.
        """
        for partition in self._partitions:
            # Partitions are iterable. They return all of their tokens.
            yield from partition

    def __str__(self) -> str:
        return ' '.join(map(lambda p: str(p), self._partitions))

    def __repr__(self):
        return (f'InvertedIndex(max_in_memory_postings = {self.max_in_memory_postings}, '
                f'min_avail_memory_perc = {self.min_avail_memory_perc}, '
                f'partitions = {self._partitions.__repr__()})')

    def _get_partition(self, token: str) -> _Partition | None:
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

        # If no existing partitions govern the token, return none.
        return None

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

            if not self._min_token or token < self._min_token:
                self._min_token = token
            if not self._max_token or token > self._max_token:
                self._max_token = token

            # If the in-memory index has become too large (too many postings or too little memory
            # available), flush (write) the index to physical disk file.
            if self._curr_in_memory_postings >= self.max_in_memory_postings or self._memory_low():
                self._flush()
        self.doc_count += 1

    def _feedr(self, root_dir_path: str | Path):
        """
        Feed a directory of documents into the inverted index, recursively searching for and adding
        documents within the root directory and its child directories.

        Args:
            root_dir_path: Pathlike to the root directory to be fed.
        """
        path = Path(root_dir_path)

        # If a JSON file was provided, use it.
        if path.is_file():
            if not path.match('*.json'):
                raise ValueError(f'{path} is not a JSON file')

            self._add(path)
            return

        # Else a dir was provided.
        # Get all nested jsons in the root dir and add it to the index.
        for file in path.rglob('*.json'):
            self._add(file)

    def _flush(self):
        """
        Write the inverted index to disk and wipe its in-memory data. The location of the data
        is somewhere in the user data i.e. C:/../AppData/Local/... in windows. This path is
        accessible via the #disk_dir property.
        """
        self._temp_file.parent.mkdir(parents = True, exist_ok = True)
        self._merge_with_disk()

        self._in_memory.clear()
        self._curr_in_memory_postings = 0

    def _merge_with_disk(self):
        temp_merged_file = self._partition_dir / 'temp_merged.json'

        with open(temp_merged_file, 'w') as out:
            # Top level object opening brace.
            out.write('{')
            # Flag used to indicate when commas should be written.
            first_entry = True
            if self._temp_file.exists():
                with open(self._temp_file, 'r') as disk_file:
                    for line in disk_file:
                        content = line.strip()

                        # Do not parse top-level JSON object lines.
                        if content in ['{', '}']:
                            continue

                        # Remove terminating comma from entry.
                        content = content[:-1] if content[-1] == ',' else content

                        # Treat the entry as its own json object to leverage python's json.loads.
                        # Probably a bit hacky but it works. We're essentially turning a key-value
                        # pair k: v into an object {k: v} and mapping it to our types.
                        entry = json.loads('{' + content + '}')
                        # Disk token and its postings.
                        token, postings = list(entry.items())[0]

                        # To maintain proper sorted order on disk, for any in-memory tokens that
                        # are less than the current disk token, write them to disk first.
                        for lesser_token in (
                                t for t in list(sorted(self._in_memory.keys())) if t < token):
                            self._dump_memory_postings(out, lesser_token, first_entry)
                            first_entry = False

                        # Combine the disk postings with in-memory postings for the same token,
                        # if any.
                        if token in self._in_memory:
                            postings.extend(
                                posting.__dict__ for posting in self._in_memory.pop(token))

                        # Write the combined token-posting entry to disk.
                        self._write_json_str_as_field(
                            out, json.dumps({token: postings}), first_entry)
                        first_entry = False

            # Flush any tokens that were not already added from memory to the end of the disk.
            # If they weren't added, it means they're greater than all tokens in disk.
            for token in list(sorted(self._in_memory.keys())):
                self._dump_memory_postings(out, token, first_entry)
                first_entry = False

            out.write('\n}')

        # Discard the old disk and replace it with this new merged disk.
        temp_merged_file.replace(self._temp_file)

    def _write_json_str_as_field(self, out: TextIO, raw: str, first_entry = False):
        """
        Write a JSON stringified object as a field.

        i.e. {k: v} becomes k: v,

        Args:
            out: The disk.
            raw: Raw stringified json.
            first_entry: First entry flag. Determines whether to prefix with a comma or not. If it's
            not the first entry, this is required for valid syntax.
        """
        prefix = '\n' if first_entry else ',\n'
        out.write(f'{prefix}{raw[1:-1]}')

    def _dump_memory_postings(self, out: TextIO, token: str, first_entry = False):
        """
        Write the postings of a token in memory to disk, and remove it from memory.

        Args:
            out: The disk.
            token: Token to dump from memory.
            first_entry: First entry flag. Whether this is the first posting being written.
        """
        # JSONify the entry and write to disk, removing from memory.
        self._write_json_str_as_field(out,
                                      json.dumps({ token: [posting.__dict__ for posting in
                                                           self._in_memory.pop(token)]}),
                                      first_entry)

    def _memory_low(self) -> bool:
        """
        Check if memory is too low.
        Returns:
            True if memory is too low, False otherwise.
        """
        # RAM & other virtual mem.
        vmem = psutil.virtual_memory()
        return vmem.percent < self.min_avail_memory_perc * 100
