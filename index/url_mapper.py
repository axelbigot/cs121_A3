import os
import json

class URLMapper:
    """
    Class that scans JSON files in a specified directory and extracts URL
    Assigns each a unique ID starting from 1 and incrementing
    """

    def __init__(self, root_path : str):
        """
        Initializes the URLMapper with the directionary containing JSON files
        Maps URL-to-ID

        :param doc_path: Path to the directory containing JSON files
        """

        self.root_path = root_path
        self.url_to_id = self.construct_mapping()
    
    def construct_mapping(self) -> dict[str, int]:
        """
        Reads all JSON files in subdirectories of the root directory,
        extracts URLs, and assigns each a unique ID

        :param: None
        :return: Dictionary mapping URLs to unique IDs
        """

        mapping = {}
        url_id = 1

        # walk through all subdirectories and files
        for subdir, _, files in os.walk(self.root_path):
            for file_name in files:
                if file_name.endswith(".json"):
                    file_path = os.path.join(subdir, file_name)

                    with open(file_path, "r", encoding="utf-8") as f:
                        try:
                            data = json.load(f)
                            url = data.get("url")

                            if url and url not in mapping:
                                mapping[url] = url_id
                                url_id += 1

                        except json.JSONDecodeError:
                            print("WARNING: " + str(file_name) + " is not a valid JSON file")

        return mapping
    
    def get_id(self, url : str) -> int:
        """
        Retrives the assigned ID for a given URL

        :param url: URL to look up in dictionary
        :return: Unique integer ID if URL found, else -1
        """

        return self.url_to_id.get(url, -1)
        