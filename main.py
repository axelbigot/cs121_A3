from index import InvertedIndex


def main():
    index = InvertedIndex()
    index.feedr('developer')
    index.flush()

if __name__ == '__main__':
    main()