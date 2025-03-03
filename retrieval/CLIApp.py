import textwrap
from enum import Enum
from pathlib import Path

from retrieval import Searcher


class State(Enum):
    """
    The various states of the CLIApp.
    """
    INIT = 'Init', # Initial state.
    INPUT = 'Input', # Obtaining query.
    RESULTS = 'Results' # Displaying results.

class CLIApp:
    """
    Simple FSM (Finite State Machine) boolean query app.

    Args:
        root_dir: Root directory of page data.
        **kwargs: Arguments passed to the inverted index.
    """
    def __init__(self, root_dir: str | Path, **kwargs):
        self._searcher = Searcher(root_dir, **kwargs) # Internal searching class.
        self._state: State = State.INIT

        # Defines which states succeed others.
        self._transition_rules: dict[State, State] = {
            State.INIT: State.INPUT,
            State.INPUT: State.RESULTS,
            State.RESULTS: State.INPUT
        }

        # Latest retrieved query input.
        self._latest_query: str | None = None

    def start(self):
        """
        Start the CLI app. It will run indefinitely until exited.
        """
        while True:
            # Start FSM.
            if self._state == State.INIT:
                self._init()
            elif self._state == State.INPUT:
                self._input()
            elif self._state == State.RESULTS:
                self._results()

            # Next state.
            self._transition()

    def _transition(self):
        """
        Transition the FSM.
        """
        self._state = self._transition_rules[self._state]

    def _results(self):
        """
        Obtain result URLs and display the top five.
        """
        results = self._searcher.search(self._latest_query)
        print('Results:')
        print(results[:5])
        print()

    def _input(self):
        """
        Obtain query input from user.
        """
        print('Enter your search query (\'exit\' to quit):\n> ', end = '')
        self._latest_query = input()

        if self._latest_query == 'exit':
            exit()

    def _init(self):
        """
        Print app boot-up screen.
        """
        print(textwrap.dedent("""
        ===========================================================================
        =============================== A3 Searcher ===============================
        ===========================================================================
        """))