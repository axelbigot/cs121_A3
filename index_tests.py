import unittest
from dataclasses import fields
from pathlib import Path

import psutil

from inverted_index import construct_index_recursively, Posting, _set_memory_low_th, InvertedIndex, \
    _FLUSH_MEMORY_THRESHOLD


def print_trunc(o, chars: int = 500):
    print(f'{str(o)[:chars]}\n\n... (truncated)')

class IndexTests(unittest.TestCase):
    def test_recursive_constructor(self):
        # Test on a subset of DEV.
        subset = 'developer/DEV/alderis_ics_uci_edu'

        index = construct_index_recursively(subset)
        print_trunc(index)

        # No token should have more postings than the number of pages.
        page_count = sum((1 for _ in Path(subset).rglob('*.json')))
        for token in index:
            self.assertLessEqual(len(index[token]), page_count)

    def test_write_index_to_disk(self):
        subset = 'developer/DEV/alderis_ics_uci_edu'

        index = construct_index_recursively(subset)
        index.flush()

        # Check that the disk was created.
        self.assertTrue(index.disk_dir.exists())
        # Check that the in-memory index was cleared
        self.assertTrue(not index._internal)

        for disk in index.disks:
            with open(disk, 'r') as f:
                content = f.read()
                print_trunc(content)

                # Check that the file contents are of posting form
                # (there are better ways to do this)
                for attr in fields(Posting):
                    self.assertIn(f'"{attr.name}":', content)

    def test_flush_index_when_low_memory(self):
        # Always flush to disk.
        threshold = 1
        old_threshold = _FLUSH_MEMORY_THRESHOLD

        try:
            _set_memory_low_th(threshold)

            subset = 'developer/DEV/alderis_ics_uci_edu'
            index = InvertedIndex()

            for page in Path(subset).rglob('*.json'):
                mem = psutil.virtual_memory()
                print(f'Available memory: {mem.available} / {mem.total} ({mem.percent}%)')

                if mem.percent < threshold * 100:
                    self.assertTrue(not index._internal)
                    print(f'Index flushed to disk ({threshold * 100}% threshold)')

                index.add(page)
        finally:
            _set_memory_low_th(old_threshold)

if __name__ == '__main__':
    unittest.main()
