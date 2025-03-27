from json import dumps as json_dumps

# from requests import get as requests_get
from dlt.sources.helpers.requests import get as requests_get

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


if __name__ == "__main__":
    with open("./data/rickandmorty/characters.jsonl", "w") as f:
        for page in get_characters():
            for character in page:
                f.write(json_dumps(character) + "\n")
