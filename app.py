import logging
import os

import pyfiglet
from flask import Flask, render_template, url_for, request, jsonify
from retrieval import Searcher


DEBUG = os.environ.get('DEBUG', 'False') == 'True'
SOURCE = os.environ.get('SOURCE', 'developer')
REBUILD = os.environ.get('REBUILD', 'False') == 'True'

logging.basicConfig(level = logging.DEBUG if DEBUG else logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

app = Flask(__name__)

if not DEBUG or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
    searcher = Searcher(
        SOURCE,
        name = 'index_main' if SOURCE == 'developer' else 'index_debug',
        persist = True,
        load_existing = not REBUILD
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
    print(pyfiglet.figlet_format('CS121 A3 G100', font = 'slant'))
    logger.debug('Started Application in DEBUG mode')

    port = int(os.environ.get('PORT', 8080))
    app.run(host = '0.0.0.0', port = port, debug=DEBUG)
