import textwrap
from enum import Enum
from pathlib import Path

from retrieval import Searcher


class State(Enum):
    INIT = 'Init',
    INPUT = 'Input',
    RESULTS = 'Results'

class CLIApp:
    def __init__(self, root_dir: str | Path, **kwargs):
        self._searcher = Searcher(root_dir, **kwargs)
        self._state: State = State.INIT

        self._transition_rules: dict[State, State] = {
            State.INIT: State.INPUT,
            State.INPUT: State.RESULTS,
            State.RESULTS: State.INPUT
        }

        self._latest_query: str | None = None

    def start(self):
        while True:
            if self._state == State.INIT:
                self._init()
            elif self._state == State.INPUT:
                self._input()
            elif self._state == State.RESULTS:
                self._results()

            self._transition()

    def _transition(self):
        self._state = self._transition_rules[self._state]

    def _results(self):
        results = self._searcher.search(self._latest_query)
        print('Results:')
        print(results[:5])
        print()

    def _input(self):
        print('Enter your search query (\'exit\' to quit):\n> ', end = '')
        self._latest_query = input()

        if self._latest_query == 'exit':
            exit()

    def _init(self):
        print(textwrap.dedent("""
        ===========================================================================
        =============================== A3 Searcher ===============================
        ===========================================================================
        """))