import bisect
import heapq
import logging
import shutil
import struct
import subprocess
import sys
import time
import json
from collections import defaultdict
from pathlib import Path
from typing import BinaryIO, Generator

import psutil

from index.JSONtokenizer import tokenize_JSON_file_with_tags
from index.defs import APP_DATA_DIR
from index.path_mapper import PathMapper
from index._simhash import simhash, calculate_similarity_score

logger = logging.getLogger(__name__)

current_dir = Path(__file__).resolve().parent

proto_file = current_dir / 'posting.proto'
logger.debug(f'Generating protobuf source classes for {proto_file}')
result = subprocess.run(
    ['protoc', f'--proto_path={current_dir}', f'--python_out={current_dir}', proto_file],
    capture_output = True, text = True)

if result.returncode != 0:
    raise RuntimeError(result.stderr)

logger.debug('Successfully generated protobuf source classes')

# These may have error squiggles, but these will resolve correctly at runtime.
# Dynamically generated classes and module from the above script. Import must be placed after
# the script.
from index.posting_pb2 import Posting, TokenEntry

_INDEXES_DIR = APP_DATA_DIR / 'indexes'

# The default number of in-memory postings before writing to disk.
# Generally, this also doubles as the max in-memory postings for any operation.
_DEFAULT_POSTINGS_FLUSH_COUNT = 5e4

# The default number of in-memory postings in each partition of the merged index.
# The actual number of postings may be higher, as token postings are not split between partitions
# (i.e. if the partition size is almost reached and a token with a massive number of entries comes
# up, that particular partition may far exceed the limit).
_DEFAULT_PARTITION_SIZE = 5e3

_WEIGHTED_TAGS = ["h1", "h2", "h3", "title", "b", "strong"]
_SIMILARITY_THRESHOLD = 0.95


class InvertedIndex:
    """
    Class representing an inverted index on disk. Provides APIs to read and write to the physical
    index.

    Args:
        root_dir: Root directory containing page .json files.
        name: Custom name for the inverted index on disk. Defaults to 'index_<obj_hash>'
        postings_flush_count In-memory posting limit restricting the max postings existing in
        memory before being flushed to disk, and the max postings that can be read from disk at a
        time.
        partition_posting_size: The max postings count for a final sorted partition.
        persist: Whether NOT to delete the index's disk once its InvertedIndex object is garbage
        collected.
        load_existing: Whether to load an existing inverted index from disk into this object, if
        a matching index exists (by name).
    """
    def __init__(self, root_dir: str | Path, *,
                 name: str = '',
                 postings_flush_count: int = _DEFAULT_POSTINGS_FLUSH_COUNT,
                 partition_posting_size: int = _DEFAULT_PARTITION_SIZE,
                 persist: bool = False,
                 load_existing: bool = False):
        logger.debug('Initializing new InvertedIndex')

        self.postings_flush_count = postings_flush_count
        self.partition_posting_size = partition_posting_size
        self.persist = persist

        self._root_dir = Path(root_dir) # Source dir for this index's pages.
        self._buf: dict[str, TokenEntry] = defaultdict(TokenEntry) # In-memory portion of the index.
        self._mapper = PathMapper(str(self._root_dir), rebuild = not load_existing) # Doc ids.
        self._postings_count = 0 # Current in-memory posting count.
        self._partition_count = 0 # Current number of partitions.
        self._page_count = 0 # Total number of pages indexed.
        self._partitions: list[Path] = [] # Index partition files.
        self._simhashes = set() # set of documents simhashes

        self._name = name # Unique name used for loading from disk, if enabled.

        if not self._name:
            self._name = f'index-{self.__hash__()}'
            logger.debug(f'No custom inverted index name provided. Defaulting to {self._name}')

        self._out_dir = _INDEXES_DIR / self._name # Location of this index on disk.
        self._merged_file = self._out_dir / f'merged.bin' # Location of the final merged index.

        # If conditions are right, build a new inverted index from scratch.
        if not load_existing or not self._out_dir.exists():
            if load_existing:
                logger.debug(f'Looked for existing InvertedIndex {self.name} from disk to load, '
                             f'but one was not found')

            logger.debug(f'Starting construction of new InvertedIndex {self.name}')
            start = time.time()

            self.build() # Build the index. Periodically flushes to disk.
            self.flush() # Flush any remaining in-memory data to disk.

            logger.debug(f'Merging {self._partition_count} partitions for InvertedIndex {self.name}')
            self._merge() # Merge partitions.

            logger.debug(f'Partitioning the (sorted) merged index for InvertedIndex {self.name}')
            self._partition()

            mins, secs = divmod(time.time() - start, 60)
            min_fmt = f'{mins}m' if mins else ''
            logger.debug(f'Finished construction of new InvertedIndex {self.name} '
                         f'in {min_fmt}{round(secs, 2)}s. '
                         f'It is now stable (read-access supported)')
        else:
            # Load an existing index from disk if available and requested.
            self.load()
            logger.debug(f'Loaded existing InvertedIndex {self.name} from disk. '
                         f'This object now manages it.')

        # Speeds up retrieval by processing min tokens of each partition from their paths, doing
        # this once instead of during each retrieval operation.
        self._token_ranges = [str(path).split('_')[-1] for path in self.disks]

    def __del__(self):
        if not self.persist:
            # Wipe this index's directory, if it exists.
            if self._out_dir.exists():
                shutil.rmtree(self._out_dir)
            logger.debug(f'Cleaned-up InvertedIndex {self.name} on disk (deleted {self._out_dir})')
        else:
            logger.debug(f'InvertedIndex {self.name} marked for persistence. '
                          f'Its disk was not deleted.')

    @property
    def name(self) -> str:
        """
        The name of the inverted index.
        """
        return self._name

    @property
    def disks(self) -> list[Path]:
        """
        Paths to the index partitions on disk.
        """
        return self._partitions

    @property
    def page_count(self) -> int:
        """
        Total number of pages indexed.
        """
        return self._page_count

    def items(self) -> Generator[tuple[str, TokenEntry], None, None]:
        """
        Get dict-style items for the tokens and postings contained in this index's disk.

        Returns:
            Generator of K, V dict entries where K = string token, V = postings list belonging to
            that token.
        """
        for disk in self.disks:
            with open(disk, 'rb') as f:
                while True:
                    try:
                        yield self._next_entry(f)
                    except StopIteration:
                        break

    def __iter__(self) -> Generator[str, None, None]:
        """
        Iterates over this index.

        Returns:
            Generator of unique tokens in this index, in order of their placement on disk.
        """
        yield from map(lambda item: item[0], self.items())

    def __getitem__(self, item: str) -> TokenEntry:
        """
        Get the postings associated with a token on disk.

        Args:
            item: The string token name.

        Returns:
            List of postings for that token.
        """
        if type(item) != str:
            raise TypeError('Indexing item must be a string.')

        for token, postings in self._iter_partition(item):
            if token == item:
                return postings

        return TokenEntry()

    def load(self):
        """
        Load an existing inverted index from disk into this object. Matches to a disk index by
        name.
        """
        if not self._out_dir.exists():
            return

        for disk in self._out_dir.glob('*.bin'):
            self._partitions.append(disk)

    def build(self):
        """
        Build the inverted index from its root_dir source.
        """
        # Delete (possibly) existing index dir, and remake it.
        if self._out_dir.exists():
            shutil.rmtree(self._out_dir)
        self._out_dir.mkdir(parents = True, exist_ok = True)

        # Add each page in the root dir.
        for page in self._root_dir.rglob('*.json'):
            # This accesses the file on disk which is inefficient bc the tokenizer does that.
            # We can move this logic to tokenizer which will improve runtime but increase coupling.
            # Putting it here because we build the index only once
            if self._is_similar(page):
                continue

            self._add_page(page)

    def flush(self):
        """
        Write the in-memory portion of the index to a partition on disk.
        """
        disk = self._out_dir / f'partition_{self._partition_count}.bin'
        self._partitions.append(disk)
        self._partition_count += 1

        self._flush_idx_data(disk, self._buf)

    def _merge(self):
        """
        Merge all partitions into a single index file using K-way merge algorithm. Maintains
        O(1) space complexity relative to total index size on disk and O(N) time complexity
        relative to total number of entries on index (N = K1 + K2 + .. + Kn, Ki ∈ Files being
        merged.)
        """
        # Open each partition file simultaneously.
        f_streams = [open(p, 'rb') for p in self._partitions]
        pq = [] # Priority Queue (min heap) to track the least token. We want to maintain sort
                # in the merged file.

        # First pass of the partition files - init the min heap with their first tokens.
        for i, stream in enumerate(f_streams):
            try:
                token, token_entry = self._next_entry(stream)
                heapq.heappush(pq, (token, i, token_entry)) # O(log N), N = heap items count.
            except StopIteration:
                pass

        last_token = None # Previous token. Used to determine when posting lists should be merged.
        batch: list[tuple] = [] # Current in-memory merged segment, written in batches.

        while pq: # Iterate until nothing left in token queue.
            token, idx, token_entry = heapq.heappop(pq) # Get the next (smallest) token entry for
                                                         # merging. O(log N), N = heap items count.
            if last_token == token:
                # Same token in two partitions - merge their postings.
                # Protobuf's MergeFrom() is O(N) for repeated fields (such as posting list), N =
                # number of repeated fields.
                batch[-1][1].df += token_entry.df
                batch[-1][1].MergeFrom(token_entry)
            else:
                # New token - append it to the batch.
                batch.append((token, token_entry))
                last_token = token # New token = new latest token.

            try:
                # Push the next token and postings to be added from this partition to the min heap.
                next_token, next_token_entry = self._next_entry(f_streams[idx])
                heapq.heappush(pq, (next_token, idx, next_token_entry))
            except StopIteration:
                pass

            new_batch = []
            # Write the batch to disk if it exceeds the in-memory postings limit.
            if len(batch) >= self.postings_flush_count:
                # Map it to a dict of helper function typing consistency.
                d: dict[str, TokenEntry] = {}
                for token, token_entry in batch:
                    if token == last_token:
                        new_batch.append((token, token_entry))
                    else:
                        d[token] = token_entry
                batch.clear()
                batch.extend(new_batch)

                if d:
                    self._flush_idx_data(self._merged_file, d)

        # Write any residual data to disk (final batch).
        if batch:
            d: dict[str, TokenEntry] = {}
            for token, token_entry in batch:
                d[token] = token_entry
            batch.clear()
            self._flush_idx_data(self._merged_file, d)

        # Close all partition disks.
        for f in f_streams:
            f.close()

        # Paritition files no longer needed - they're all merged.
        for partition in self._partitions:
            partition.unlink(missing_ok = True)

        # Index now consists solely of a single merged file.
        self._partitions = [self._merged_file]
        self._partition_count = 0

    def _flush_idx_data(self, disk: Path, data: dict[str, TokenEntry]):
        """
        Flush arbitrary inverted index data to disk.

        Args:
            disk: The disk file path to write to.
            data: Inverted index data in memory.
        """
        logger.debug(f'Flushing {sys.getsizeof(data) / 1024}KB from memory to {disk}')
        logger.debug(f'{psutil.virtual_memory().percent}% virtual memory currently used')

        with open(disk, 'ab+') as f:
            for token, token_entry in sorted(data.items()):
                # Map to protobuf types and serialize.
                entry = TokenEntry(df = token_entry.df, postings = token_entry.postings)
                entry_data = entry.SerializeToString()

                # Write the length of the token. Needed to efficiently stream data token-by-token.
                # We need to know where one token entry ends and another starts; these are variable.
                f.write(struct.pack('I', len(token)))
                # Write the token.
                f.write(token.encode('utf-8'))
                # Write the size of the postings.
                f.write(struct.pack('I', len(entry_data)))
                # Write the serialized postings.
                f.write(entry_data)

        data.clear()

    def _partition(self):
        """
        Partition the merged inverted index file to several smaller files. These partitions are
        lexicographically ordered. Runs in O(N) time relative to the size of the merged index.
        """
        merged = self.disks[0]
        self._postings_count = 0
        with open(merged, 'rb') as f:
            while True: # Runs until there are no more entries.
                try:
                    # Appends tokens to the in-memory buffer while under the partition size
                    # threshold.
                    while self._postings_count < self.partition_posting_size:
                        token, token_entry = self._next_entry(f)
                        self._buf[token] = token_entry
                        self._postings_count += len(token_entry.postings)
                except StopIteration:
                    # No more entries.
                    break
                finally:
                    # Write in-memory partition to disk.
                    min_token = sorted(self._buf.keys())[0]
                    name = f'partition_{min_token}.bin'
                    path = self._out_dir / name

                    self._flush_idx_data(path, self._buf)
                    self._partitions.append(path)
                    self._postings_count = 0

        merged.unlink()
        self._partitions.remove(merged)

    def _get_partition_file(self, token: str) -> Path:
        """
        Gets the partition file that contains the provided token, if the token exists (i.e. the
        token is in this partition, or it is nowhere in the index). Uses binary search to find
        the right partition in O(log N) time, N = number of partitions.

        Args:
            token: Token whose partition path to retrieve.

        Returns:
            Path to partition file.
        """
        index = bisect.bisect_left(self._token_ranges, token)
        return self.disks[index - 1] if index else self.disks[0]

    def _iter_partition(self, token: str) -> Generator[tuple[str, list], None, None]:
        """
        Iterates over all token-posting items in specified token's partition.

        Args:
            token: The token whose partition to iterate over.

        Returns:
            Generator of token-posting tuples.
        """
        partition = self._get_partition_file(token)
        with open(partition, 'rb') as f:
            while True:
                try:
                    yield self._next_entry(f)
                except StopIteration:
                    break

    def _next_entry(self, f: BinaryIO) -> tuple[str, TokenEntry]:
        """
        Get the next token-postings entry in an opened binary file.

        Args:
            f: File openend in binary read mode.

        Returns:
            Tuple entry of (token, postings) for the next token-postings entry in the file.
        """
        length_bytes = f.read(4) # Encoded length of the token.
        if not length_bytes: # Might not be any more tokens.
            raise StopIteration

        try:
            token_length = struct.unpack('I', length_bytes)[0] # Decode token length.
            token = f.read(token_length).decode('utf-8') # Decode token.

            token_entry_length = struct.unpack('I', f.read(4))[0] # Decode posting list length.
            token_entry_data = f.read(token_entry_length) # Decode posting list.

            # Deserialize postings to protobuf types.
            token_entry = TokenEntry()
            token_entry.ParseFromString(token_entry_data)

            return token, token_entry
        except:
            # If something goes wrong, can't reliably parse the file further.
            raise StopIteration

    def _add_page(self, page: Path):
        """
        Add a page to the index.

        Args:
            page: Path to json response file.
        """
        self._page_count += 1
        doc_id = self._mapper.get_id(str(page))
        for token, tag_freqs in tokenize_JSON_file_with_tags(page, _WEIGHTED_TAGS).items():
            self._buf[token].postings.append(Posting(
                doc_id = doc_id,
                frequency = sum(tag_freqs.values()), # Kept for now for compatibility.
                tag_frequencies = tag_freqs))
            self._buf[token].df += 1
            self._postings_count += 1

            # Flush to disk if in-memory index grows too large.
            if self._postings_count >= self.postings_flush_count:
                self._postings_count = 0
                self.flush()
                self._buf.clear()

    def _is_similar(self, page: Path) -> bool:
        """
        Returns True if provided content is similar to another document.
    
        Args:
            page: Path to the json of a document
    
        Returns:
            bool: if the document is similar to another one
        """
        with open(page, 'r') as file:
            html = json.load(file)['content']
            hashed_doc = simhash(html)
        
            if hashed_doc in self._simhashes:
                return True
        
            # TODO: with a big index, we may not be able to hold every single simhash in memory
            for explored_hash in self._simhashes:
                sim = calculate_similarity_score(hashed_doc, explored_hash)
                if sim >= _SIMILARITY_THRESHOLD:
                    return True
    
        return False
