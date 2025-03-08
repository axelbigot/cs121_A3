import cProfile
import pstats
import shutil
import threading
import time
import unittest
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from types import MethodType
from unittest.mock import patch

import psutil

from JSONtokenizer import compute_word_frequencies, tokenize_JSON_file
from index import InvertedIndex
from index.inverted_index import _WEIGHTED_TAGS
from index.JSONtokenizer import tokenize_JSON_file_with_tags
from index.posting_pb2 import Posting
from path_mapper import PathMapper
from retrieval import CLIApp


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
    def test_index_all_tokens_present(self):
        """
        Tests that all tokens are reflected and accessible from the inverted index.
        """
        datasource = SMALL_DATASET
        index = InvertedIndex(
            datasource,
            name = 'test_index_all_tokens_present',
            postings_flush_count = 500,
            persist = True
        )

        expected_tokens = set()
        for p in Path(datasource).rglob('*.json'):
            expected_tokens.update(set(compute_word_frequencies(tokenize_JSON_file(str(p))).keys()))

        self.assertEqual(expected_tokens, set(index))

    def test_index_all_postings_present(self):
        """
        Tests that all postings are accurately present in the inverted index, for each token.
        """
        datasource = SMALL_DATASET
        index = InvertedIndex(
            datasource,
            name = 'test_index_all_postings_present',
            postings_flush_count = 500,
            persist = True
        )

        in_memory_index: dict[str, list[Posting]] = defaultdict(list)
        mapper = PathMapper(str(Path(datasource)))
        for p in Path(datasource).rglob('*.json'):
            doc_id = mapper.get_id(str(p))
            for token, tag_freqs in tokenize_JSON_file_with_tags(str(p), _WEIGHTED_TAGS).items():
                in_memory_index[token].append(Posting(
                    doc_id = doc_id,
                    frequency = sum(tag_freqs.values()),
                    tag_frequencies = tag_freqs))

        self.assertEqual(sorted(in_memory_index.items()), list(index.items()))

    def test_index_get_postings_by_token(self):
        """
        Tests that the postings of an individual token can be obtained.
        """
        datasource = SMALL_DATASET
        index = InvertedIndex(
            datasource,
            name = 'test_index_get_postings_by_token',
            postings_flush_count = 500,
            persist = True
        )

        target_token = 'alderis'
        expected_postings: list[Posting] = []
        mapper = PathMapper(str(Path(datasource)))
        for p in Path(datasource).rglob('*.json'):
            doc_id = mapper.get_id(str(p))
            for token, tag_freqs in tokenize_JSON_file_with_tags(str(p), _WEIGHTED_TAGS).items():
                if token == target_token:
                    expected_postings.append(Posting(
                        doc_id = doc_id,
                        frequency = sum(tag_freqs.values()),
                        tag_frequencies = tag_freqs))

        self.assertEqual(expected_postings, list(index[target_token]))

    @patch('builtins.input', side_effect=[
        'alderis', 'brain cat dog', 'the', 'zhu', 'a', 'master of software engineering',
        'uci', 'ics', 'irvine', 'exit']
    )
    def test_index_performance(self, _):
        profiler = cProfile.Profile()
        profiler.enable()

        try:
            CLIApp(
                '../developer',
                name = 'index_main',
                load_existing = True,
                persist = True
            ).start()
        except SystemExit:
            pass

        profiler.disable()

        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('tottime').print_stats(20)

        stats_dir = Path('../build/stats/test_index_performance')
        shutil.rmtree(stats_dir, ignore_errors=True)
        stats_dir.mkdir(exist_ok = True, parents = True)

        profiler.dump_stats(stats_dir / f'profile_{datetime.now()
                            .strftime("%m-%d_%H-%M-%S")}.prof')

if __name__ == '__main__':
    unittest.main()
