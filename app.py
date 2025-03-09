import argparse
import logging
import os

import pyfiglet
from flask import Flask, render_template, url_for, request, jsonify
from pathlib import Path
from retrieval import Searcher

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

app = Flask(__name__)

searcher = Searcher(
    args.source,
    name = 'index_main' if args.source == 'developer' else 'index_debug',
    persist = True,
    load_existing = not args.rebuild
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('query', '')

    if not query:
        return render_template('results.html', results=[], search_time='')

    results, search_time = searcher.search(query)
    return render_template('results.html', results = results[:len(results)], search_time=search_time)
    
if __name__ == '__main__':
    print(pyfiglet.figlet_format('CS121 A3 G100', font = 'slant'))
    logger.debug('Started Application in DEBUG mode')

    port = int(os.environ.get('PORT', 8080))
    app.run(host = '0.0.0.0', port = port, debug=True)
