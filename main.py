from index import InvertedIndex, generate_analysis


def main():
    index = InvertedIndex('developer')
    generate_analysis(index)

if __name__ == '__main__':
    main()