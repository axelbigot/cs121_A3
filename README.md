# A3 Search Engine
Group 100

## Requirements

- Python 3.12+
- Protoc

1. Download requirements using `pip install -r requirements.txt`
2. Download [protoc](https://grpc.io/docs/protoc-installation/)

Verify protoc is installed by running `protoc --version`. It must be on your PATH. You may need to restart your IDE and terminal.

## Running the Application

Run the application locally using `python app.py`

This will launch a lightweight flask app on port 8080. Making a query should be self-explanatory once the you open the GUI. Just enter your query and press enter.

It is recommended to run the app in debug mode to observe our index build step-by-step.
We provide detailed logs showing writes to disk and changes in virtual memory.

## Protoc

We use the protobuf compiler (protoc) command line tool to generate python classes from protobuf definitions.
Protobuf is a high-compression, fast-access binary serialization protocol that is used during index flushing to disk.

Protoc will be invoked during application start.

## Run Options

All options, including debug, are set using environment variables:

| Option Name              | Kind     | Default     | Description                                                                                                                                                                                                                                                                                             |
|--------------------------|----------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DEBUG`                  | Boolean  | `False`     | Enables detailed debug logs.                                                                                                                                                                                                                                                                            |
| `REBUILD`                | Boolean  | `False`     | Forces rebuilding of the index from scratch, even if one exists on disk                                                                                                                                                                                                                                 |
| `SOURCE`                 | String   | `developer` | Path to the index source (the json pages).                                                                                                                                                                                                                                                              |
| `NO_DUPLICATE_DETECTION` | Boolean  | `True`      | Disables near and exact duplicate page detection and elimination. This is true by default (disabled) as it makes index building a lot slower due to the underlying simhash calls, but feel free to enable it on a smaller index to verify. Near-duplicate elimination IS enabled in our deployed server |
| `USE_SPELLCHECK`         | Boolean  | `False`     | Enables spellcheck and typo correction. Disabled by default as it makes some queries a lot slower. Not enabled in our deployed version                                                                                                                                                                  |

You can configure these however you'd prefer. We recommend using an IDE like Pycharm to customize run configurations, but
you can also manually export these variables if you'd like to change their values.

We again recommend using `DEBUG` and `REBUILD` (if you already built the index once) to see index construction.

## Disk Structure

All disk data pertaining to this app is stored in your machine's local app data directory under `CS121_A3`.
So the full path to this app data is `<LOCAL_APP_DATA_DIR>/CS121_A3`.

In this directory, there will be 3 subdirectories after building for the first time:

- `mappers/` which store our `PathMapper` objects (url to id mappers, and others). These were saved to disk for convenience and DX, as to not have to rebuild them every time.
- `searchers/` which store our `Searcher` objects, which contain precomputed cosine similarity vectors.
- `indexes/` which contain the actual disk inverted indexes. This directory will contain further subdirectories for any indexes built. If you built using the default `SOURCE`, you'll see an `index_main` subdirectory. If you built using a custom `SOURCE`, you'll see an `index_debug` subdirectory.

Each index subdirectory contains binary partition files which segment our index by token ranges. These have the naming form: `partition_<smallest_token_in_file>.bin`.

## Index Construction Overview

Our index construction process is like so:
1. Tokenize pages into memory.
2. Sort by token lexicographically and dump to disk once in-memory threshold is reached
   1. Note that each partition here is sorted (tokens in ASC order).
3. Repeat until all pages processed
4. Perform K-way merging of all initial partitions to a `merged.bin` file
5. Split the `merged.bin` file into the final partitions by token range.

We maintain constant space complexity regardless of the total index size at all steps.
