import argparse
import logging
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
    'developer',
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
    page = int(request.args.get('page', 1))
    results_per_page = 5

    if not query:
        return render_template('results.html', results=[], search_time='', page=page)

    results, search_time = searcher.search(query)

    start_index = (page - 1) * results_per_page
    end_index = start_index + results_per_page
    paginated_results = results[start_index : end_index]

    summary = "this is a test " * 50

    return render_template('results.html', results=paginated_results, search_time=search_time, page=page, total_results=len(results), results_per_page=results_per_page, summary=summary)
    
if __name__ == '__main__':
    print(pyfiglet.figlet_format('CS121 A3 G100', font='slant'))
    logger.debug('Started Application in DEBUG mode' if args.debug else 'Started Application in NORMAL mode')

    app.run(debug=args.debug)
