# A3 Search Engine
Group 100

## Running the Application

Run the application from the commandline using `python main.py`

The program specifies some helpful CLI options, described below:

| Option Name(s)   | Values              | Default     | Description                                                                                                |
|------------------|---------------------|-------------|------------------------------------------------------------------------------------------------------------|
| `-d` `--debug`   | Flag                | No debug    | Enables debug logging                                                                                      |
| `-s` `--source`  | Path to a directory | `developer` | Specifies the source directory containing JSON page responses for the inverted index.                      |
| `-r` `--rebuild` | Flag                | No rebuild  | Force rebuild the index from scratch. Without this, an existing index will try to be found before building |

Example running in debug mode, using a custom source dir, and forced rebuild:
`python main.py -d -r -s ./small_index`
