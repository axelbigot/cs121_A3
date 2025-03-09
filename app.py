import os

from flask import Flask, render_template, url_for, request, jsonify
from pathlib import Path
from retrieval import Searcher

app = Flask(__name__)

searcher = Searcher(
    'developer',
    name = 'index_main',
    persist = True,
    load_existing = True
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
    port = int(os.environ.get('PORT', 8080))
    app.run(host = '0.0.0.0', port = port, debug=True)
