import threading
import time
import unittest
from dataclasses import fields
from datetime import datetime
from types import MethodType

from inverted_index import *


def print_trunc(o, chars: int = 500):
    """
    Print, but truncate.

    Args:
        o: Object to print.
        chars: Length at which to truncate
    """
    s = str(o)
    print(f'{s[:chars]}\n\n... truncated at {chars} chars ({len(s) - chars} more hidden)')

def wrap(func, before = None, after = None):
    """
    Wrap a method. This overwrites the original method on its object instance.

    Args:
        func: The method to wrap.
        before: Function to run before the original method is invoked.
        after: Function to run after the original method is invoked.
    """
    obj = func.__self__

    def wrapped(self, *args, **kwargs):
        if before:
            before(*args, **kwargs)
        res = func(*args, **kwargs)
        if after:
            after(*args, **kwargs)
        return res

    wrapped.__name__ = func.__name__

    setattr(obj, func.__name__, MethodType(wrapped, obj))

def log_call(func):
    """
    Print out all invocations of func.

    Args:
        func: The function to log calls.
    """
    def after(*args, **kwargs):
        print(f'[{datetime.now().strftime('%H:%M:%S')}] '
              f'{f'{func.__self__.__class__.__name__}.' if func.__self__ else ''}'
              f'{func.__name__}({args}, {kwargs}) called')

    wrap(func, after = after)

class MemPoll:
    """
    Context manager that prints out memory stats every second while alive. Operates in a background
    thread.
    """
    def __init__(self):
        self.stop = threading.Event()

    def __enter__(self):
        def poll(stop):
            while not stop.is_set():
                mem = psutil.virtual_memory()
                print(f'Available memory: {mem.available} / {mem.total} ({mem.percent}%)')
                time.sleep(1)

        threading.Thread(target = poll, args = (self.stop,), daemon = True).start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop.set()

class IndexTests(unittest.TestCase):
    def test_partition_cross_component_retrieval(self):
        """
        Tests that posting retrieval from partitions searches both physical and virtual memory.
        """
        # Get some partition from the index.
        index = InvertedIndex()
        partition = index._misc_partition
        token = 'test'

        postings = [Posting(doc_id = 0, frequency = 5), Posting(doc_id = 0, frequency = 4)]

        # Add a posting and flush to disk.
        partition.add(token, postings[0])
        partition.flush()

        # Add another posting for the same token and do not flush to disk.
        partition.add(token, postings[1])

        # Ensure postings are not duplicated.
        self.assertEqual(len(postings), len(partition.get(token)))

        # Ensure all added postings are retrieved.
        for posting in postings:
            self.assertIn(posting, partition.get(token))

    def test_inverted_index_feedr(self):
        """
        Tests the use of feedr. Ensures that no unexpected behavior occurs.
        """
        test_cases = [
            ('developer/DEV/alderis_ics_uci_edu', 100, 'small_index'),
            # ('developer', 10 ** 6, 'large_index') # Takes long as hell
        ]

        for subset, max_postings, index_id in test_cases:
            with self.subTest(name = index_id):
                with MemPoll() as _:
                    index = InvertedIndex(index_id, max_in_memory_postings = max_postings)
                    print(f'Constructing {index.__repr__()}')

                    log_call(index.flush)

                    index.feedr(subset)
                    print_trunc(index)

                    # No token should have more postings than the number of pages.
                    page_count = sum((1 for _ in Path(subset).rglob('*.json')))
                    for token in index:
                        self.assertLessEqual(
                            len(index[token]), page_count,
                            f'Token <{token}> has more postings than there are pages. '
                            f'This is impossible.')

    def test_write_index_flush(self):
        """
        Tests that flushing to disk works as expected.
        """
        files = Path('../developer/DEV/alderis_ics_uci_edu').glob('*.json')

        # No posting limit to allow manual flushing for this test.
        index = InvertedIndex(max_in_memory_postings = 99999)
        # Just add a single page to disk.
        index.add(next(files))
        index.flush()

        # Check that the disk was created.
        self.assertTrue(index.partition_dir.exists())
        # Check that the in-memory index was cleared
        self.assertTrue(not index._misc_partition._in_memory)

        for disk in index.disk_partitions:
            with open(disk, 'r') as f:
                content = f.read()
                print_trunc(content)

                # Check that the file contents are of posting form (there are better ways to do
                # this, principally schema validation but would be overkill).
                for attr in fields(Posting):
                    self.assertIn(f'"{attr.name}":', content)

    def test_flush_index_when_low_memory(self):
        # Flush to disk when <= 100% of mem (means it will flush on every addition).
        threshold = 1
        print(f'Threshold: {threshold * 100}%')

        # Print available memory in the background.
        with MemPoll() as _:
            subset = 'developer/DEV/ugradforms_ics_uci_edu'

            index = InvertedIndex(
                'flush_on_memory',
                # Make sure no flushing occurs due to posting limits. Purely test memory limits.
                max_in_memory_postings = 999999999,
                min_avail_memory_perc = threshold
            )

            # Log when flushing occurs.
            log_call(index.flush)

            # Record if a flush happened.
            flush_called = False
            def set_called_flag():
                nonlocal flush_called
                flush_called = True
            wrap(index.flush, after = set_called_flag)

            # Build index.
            index.feedr(subset)

            # Flush should have been called at least once since we are violating memory threshold
            # (100%) every time.
            self.assertTrue(flush_called, 'InvertedIndex.flush was never called')

if __name__ == '__main__':
    unittest.main()
