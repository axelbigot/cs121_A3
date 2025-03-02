import argparse
import logging

import pyfiglet

from index import InvertedIndex, generate_analysis


parser = argparse.ArgumentParser(description = 'Main entry point for the A3 Search Engine')
parser.add_argument('-d', '--debug', action = 'store_true', help = 'Enable debug mode')
args = parser.parse_args()

logging.basicConfig(level = logging.DEBUG if args.debug else logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def main():
    print(pyfiglet.figlet_format('CS121 A3 G98', font = 'slant'))
    logger.debug('Started Application in DEBUG mode')

    index = InvertedIndex(
        'developer',
        name = 'index_main',
        persist = True
    )
    generate_analysis(index)

if __name__ == '__main__':
    main()
