import logging
import os
from pathlib import Path

import pyfiglet
from flask import Flask, render_template, url_for, request, jsonify
from retrieval import Searcher, Summarizer
from retrieve_index import download_and_unzip_source, download_and_unzip_prebuilt_index


# Deployment flags. Irrelevant unless you want to run deployment environment locally.
# Note this enabling these may cause failure, as this is not intended to be run locally.
# See Dockerfile.
PROD = os.environ.get('PROD', 'False') == 'True' # Production deployment flag.
PROD_PREBUILT = os.environ.get('PROD_PREBUILT', 'False') == 'True' # Flag to get prebuilt index
                                                                   # from S3.

DEBUG = os.environ.get('DEBUG', 'False') == 'True' # Enable debug logging.
SOURCE = os.environ.get('SOURCE', 'developer') # Set target source directory (JSON pages).
REBUILD = os.environ.get('REBUILD', 'False') == 'True' # Rebuilt from scratch.

# Feature flags.
# Enable duplicate page detection and elimination - slows index build.
NO_DUPLICATE_DETECTION = os.environ.get('NO_DUPLICATE_DETECTION', 'True') == 'True'
# Enable spellchecking - slows querying.
USE_SPELLCHECK = os.environ.get('USE_SPELLCHECK', 'False') == 'True'

logging.basicConfig(level = logging.DEBUG if DEBUG else logging.INFO,
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

if PROD_PREBUILT and not REBUILD and SOURCE == 'developer':
    logger.debug('Retrieving prebuilt disks from S3')
    download_and_unzip_prebuilt_index()
elif PROD and not Path(SOURCE).exists():
    logger.debug('Retrieving source pages from S3')
    download_and_unzip_source()

app = Flask(__name__)

searcher = Searcher(
    SOURCE,
    use_spellcheck = USE_SPELLCHECK,
    name = 'index_main' if SOURCE == 'developer' else 'index_debug',
    persist = True,
    load_existing = not REBUILD,
    no_duplicate_detection = NO_DUPLICATE_DETECTION,
)

summarizer = Summarizer()

@app.template_filter('zip')
def zip_lists(a, b):
    return zip(a, b)

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

    return render_template(
        'results.html',
        results=paginated_results,
        search_time=search_time,
        page=page,
        total_results=len(results),
        results_per_page=results_per_page
    )

@app.route('/result-details', methods=['GET'])
def summary():
    doc_id = int(request.args.get('doc_id'))
    return summarizer.getSummary(searcher.path_mapper.get_path_by_id(doc_id))
    
if __name__ == '__main__':
    print(pyfiglet.figlet_format('CS121 A3 G100', font = 'slant'))
    logger.debug('Started Application in DEBUG mode')

    port = int(os.environ.get('PORT', 8080))
    app.run(host = '0.0.0.0', port = port)
