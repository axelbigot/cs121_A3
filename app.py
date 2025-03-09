import logging
import os

import pyfiglet
from flask import Flask, render_template, url_for, request, jsonify
from retrieval import Searcher


DEBUG = os.environ.get('DEBUG', 'False') == 'True'
SOURCE = os.environ.get('SOURCE', 'developer')
REBUILD = os.environ.get('REBUILD', 'False') == 'True'

logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

app = Flask(__name__)

if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
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

    if not query:
        return render_template('results.html', results=[], search_time='')

    results, search_time = searcher.search(query)
    return render_template('results.html', results = results[:len(results)], search_time=search_time)
    
if __name__ == '__main__':
    print(pyfiglet.figlet_format('CS121 A3 G100', font = 'slant'))
    logger.debug('Started Application in DEBUG mode')

    port = int(os.environ.get('PORT', 8080))
    app.run(host = '0.0.0.0', port = port, debug=True)
