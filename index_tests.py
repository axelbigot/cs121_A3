import unittest
from dataclasses import fields
from pathlib import Path

from inverted_index import construct_index_recursively, Posting


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

if __name__ == '__main__':
    unittest.main()
