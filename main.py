import argparse
import logging

import pyfiglet

from retrieval import CLIApp


parser = argparse.ArgumentParser(description = 'Main entry point for the A3 Search Engine')
parser.add_argument('-d', '--debug', action = 'store_true', help = 'Enable debug mode')
parser.add_argument('-s', '--source', type = str, default = 'developer',
                    help = 'The source directory of pages for the inverted index (default: ./developer).')
parser.add_argument('-r', '--rebuild', action = 'store_true',
                    help = 'Whether to rebuild the inverted index.')
args = parser.parse_args()

logging.basicConfig(level = logging.DEBUG if args.debug else logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def main():
    print(pyfiglet.figlet_format('CS121 A3 G100', font = 'slant'))
    logger.debug('Started Application in DEBUG mode')

    CLIApp(
        args.source,
        name = 'index_main' if args.source == 'developer' else 'index_debug',
        persist = True,
        load_existing = not args.rebuild
    ).start()

if __name__ == '__main__':
    main()
