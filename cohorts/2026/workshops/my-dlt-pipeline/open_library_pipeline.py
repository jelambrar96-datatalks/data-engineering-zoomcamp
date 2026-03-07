"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


# if no argument is provided, `access_token` is read from `.dlt/secrets.toml`
@dlt.source
def open_library_rest_api_source(
    bibkeys: str = "ISBN:0451526538",
    access_token: str = dlt.secrets.value
):
    """Define dlt resources from REST API endpoints.

    Parameters
    ----------
    bibkeys: str
        The `bibkeys` query argument used by the Open Library `/api/books` endpoint.
        Defaults to a single ISBN that is guaranteed to return data.
    access_token: str
        Not used by the public Open Library API but kept for compatibility with
        the skeleton template. Ignored.
    """
    # open library API does not require authentication for the public endpoints
    config: RESTAPIConfig = {
        "client": {
            # base URL for the Open Library REST API
            "base_url": "https://openlibrary.org/",
            # authentication is not used in the public API; remove the block
            # "auth": {"type": "bearer", "token": access_token},
        },
        # define one resource for the `books` endpoint
        # see https://openlibrary.org/dev/docs/api/books
        "resources": [
            {
                "name": "books",
                # endpoint configuration is placed under the `endpoint` key
                "endpoint": {
                    # the API path relative to base_url
                    "path": "api/books",
                    # default parameters - the pipeline will fetch this book
                    "params": {
                        "bibkeys": bibkeys,
                        "format": "json",
                        "jscmd": "data",
                    },
                    # the API returns a mapping keyed by bibkeys rather than an
                    # array; instruct dlt to use the entire JSON response as
                    # the dataset
                    "data_selector": "$",
                    # we are not using paging/incremental loading for now
                }
            }
        ],
        # you can set global defaults (e.g. timeout, headers) here if needed
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
