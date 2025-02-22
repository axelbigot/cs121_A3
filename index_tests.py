import unittest
from pathlib import Path

from inverted_index import construct_index_recursively


class IndexTests(unittest.TestCase):
    def test_recursive_constructor(self):
        # Test on a subset of DEV.
        subset = 'developer/DEV/alderis_ics_uci_edu'

        index = construct_index_recursively(subset)
        print(f'{str(index)[:500]}\n\n... (truncated)')

        # No token should have more postings than the number of pages.
        page_count = sum((1 for _ in Path(subset).rglob('*.json')))
        for token in index:
            self.assertLessEqual(len(index[token]), page_count)

if __name__ == '__main__':
    unittest.main()
