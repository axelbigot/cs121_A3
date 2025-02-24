import threading
import time
import unittest
from datetime import datetime
from types import MethodType

from index.inverted_index import _Partition, _INDEXES_DIR
from index.inverted_index import *


SMALL_DATASET = '../developer/DEV/alderis_ics_uci_edu'

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
    def test_partition_retrieval(self):
        """
        Tests retrieval of postings for a token in a partition.
        """
        token = 'test'
        postings = [Posting(doc_id = 0, frequency = 5), Posting(doc_id = 0, frequency = 4)]

        partition = _Partition(_INDEXES_DIR / 'test_partition_retrieval',
                               {token: postings})

        # Ensure postings are not duplicated.
        self.assertEqual(len(postings), len(partition.get(token)))

        disk_postings = list(partition.get(token))

        # Ensure all added postings are retrieved.
        for posting in postings:
            self.assertIn(posting, disk_postings)

    def test_index_retrieval(self):
        """
        Tests retrieval/iteration of all tokens in the index.
        """
        file = next(Path(SMALL_DATASET).glob('*.json'))
        index = InvertedIndex(file)
        expected_tokens = set(tokenize_JSON_file(file))

        tokens = list(index)
        self.assertEqual(len(expected_tokens), len(tokens))
        for token in tokens:
            self.assertIn(token, expected_tokens)

    def test_inverted_index_construction(self):
        """
        Tests construction of an index. Ensures that no unexpected behavior occurs.
        """
        test_cases = [
            (SMALL_DATASET, 100, 'small_index'),
            # ('developer', 10 ** 6, 'large_index') # Takes long as hell
        ]

        for subset, max_postings, index_id in test_cases:
            with self.subTest(name = index_id):
                with MemPoll() as _:
                    index = object.__new__(InvertedIndex)
                    log_call(index._flush)

                    index.__init__(subset, index_id, max_in_memory_postings = max_postings)

                    print_trunc(index)

                    # No token should have more postings than the number of pages.
                    page_count = sum((1 for _ in Path(subset).rglob('*.json')))
                    for token in index:
                        self.assertLessEqual(
                            len(index[token]), page_count,
                            f'Token <{token}> has more postings than there are pages. '
                            f'This is impossible.')

    def test_flush_index_when_low_memory(self):
        """
        Tests that flushing to disk occurs if memory is low by setting an artificially high
        threshold of 100%.
        """
        # Flush to disk when <= 100% of mem (means it will flush on every addition).
        threshold = 1
        print(f'Threshold: {threshold * 100}%')

        # Print available memory in the background.
        with MemPoll() as _:
            subset = 'developer/DEV/ugradforms_ics_uci_edu'

            index = object.__new__(InvertedIndex)
            # Log when flushing occurs.
            log_call(index._flush)

            # Record if a flush happened.
            flush_called = False

            def set_called_flag():
                nonlocal flush_called
                flush_called = True

            wrap(index._flush, after = set_called_flag)

            index.__init__(
                subset,
                'flush_on_memory',
                # Make sure no flushing occurs due to posting limits. Purely test memory limits.
                max_in_memory_postings = 999999999,
                min_avail_memory_perc = threshold
            )

            # Flush should have been called at least once since we are violating memory threshold
            # (100%) every time.
            self.assertTrue(flush_called, 'InvertedIndex.flush was never called')

if __name__ == '__main__':
    unittest.main()
