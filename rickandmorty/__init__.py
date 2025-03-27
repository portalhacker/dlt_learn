from json import dumps as json_dumps
from os import environ as os_environ
from time import time

from dlt.sources.helpers.requests import get as requests_get
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# from requests import get as requests_get
from dlt.sources.rest_api import rest_api_source

os_environ["RUNTIME__LOG_LEVEL"] = "INFO"

BASE_API_URL = "https://rickandmortyapi.com/api"


def get_characters():
    page_number = 1

    while True:
        params = {"page": page_number}

        response = requests_get(f"{BASE_API_URL}/character", params=params)
        response.raise_for_status()
        page_json = response.json()
        print(f"✅ Got page {page_number} with {len(page_json['results'])} results")

        if page_json["info"]["next"] != None:
            yield page_json["results"]
            page_number += 1
        else:
            break


def get_characters_dlt():
    client = RESTClient(
        base_url=BASE_API_URL,
        paginator=PageNumberPaginator(base_page=1, total_path="info.pages"),
    )
    for page in client.paginate("/character"):
        yield page


rickandmorty_source = rest_api_source(
    {
        "client": {
            "base_url": BASE_API_URL,
            "paginator": {
                "type": "page_number",
                "base_page": 1,
                "total_path": "info.pages",
            },
        },
        "resources": [
            "character",
        ],
    }
)


if __name__ == "__main__":
    with open("./data/rickandmorty/characters.jsonl", "w") as f:
        start_time = time()
        for page in get_characters_dlt():
            for character in page:
                f.write(json_dumps(character) + "\n")
        print(f"✅ Done in {time() - start_time} seconds")
