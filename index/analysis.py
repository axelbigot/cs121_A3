import os

def generate_analysis(InvertedIndex) -> None:
    print(f"Analysis of InvertedIndex with name: {InvertedIndex.name}")
    print("-" * 40)
    print(f"# of indexed documents: {InvertedIndex.page_count}")
    print(f"# of unique words: {sum(1 for _ in InvertedIndex)}")
    print(f"Total size of index on disk: {sum(os.path.getsize(path) for path in InvertedIndex.disks) / 1024} KB")

