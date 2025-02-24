from index import InvertedIndex


def main():
    index = InvertedIndex('developer/DEV/alderis_ics_uci_edu', max_in_memory_postings = 500)

if __name__ == '__main__':
    main()