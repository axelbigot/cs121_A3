from inverted_index import InvertedIndex
import os

def generate_analysis(InvertedIndex) -> None:
    print(f"Analysis of InvertedIndex with id: {InvertedIndex.id}")
    print("-" * 40)
    print(f"# of indexed documents: {InvertedIndex.doc_count}")
    print(f"# of unique words: {sum(1 for _ in InvertedIndex)}")
    print(f"Total size of index on disk: {sum(os.path.getsize(path) for path in InvertedIndex.disk_partitions) / 1024} KB")   

