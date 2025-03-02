from retrieval.searcher import search

def print_banner():
    banner = """
==========================================================
    A3 Search Engine CLI Interface
==========================================================

Enter 'exit' to quit program.

==========================================================
    """
    print(banner)

def get_query():
    print("Enter your search:")
    query = input("> ")
    return query

def print_results(results):
    print("Results: ")
    if results:
        for i, result in enumerate(results):
            if i == len(results) - 1:
                print(result)
            else:
                print(f"{result}, ", end='')
    else:
        print("None found")

def search(query):
    return ["hello","world", "!.com"]

def run_interface():
    print_banner()
    query = get_query()
    print()
    while(query != "exit"):
        results = search(query)
        print_results(results)
        print()
        query = get_query()

run_interface()