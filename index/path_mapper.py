import os
import json

class PathMapper:
    """
    Class that scans JSON files in a specified directory and assigns each 
    file path a unique ID starting from 1 and incrementing
    """

    def __init__(self, root_path : str):
        """
        Initializes the PathMapper with the dictionary containing JSON files
        Maps Path-to-ID and URL-to-ID

        :param doc_path: Path to the directory containing JSON files
        """

        self.root_path = root_path
        self.path_to_id, self.url_to_id = self.construct_mapping()
    
    def construct_mapping(self) -> dict[str, int]:
        """
        Reads all JSON files in subdirectories of the root 
        directory and assigns each a unique ID

        :param: None
        :return: Dictionary mapping file paths to unique IDs
        """

        path_to_id = {}
        url_to_id = {}
        file_id = 1

        # walk through all subdirectories and files
        for subdir, _, files in os.walk(self.root_path):
            for file_name in files:
                if file_name.endswith(".json"):
                    file_path = os.path.join(subdir, file_name)

                    path_to_id[file_path] = file_id

                    # attempt to extract URL from JSON
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            url = data.get("url")

                            if url and url not in url_to_id:
                                url_to_id[url] = file_id
                    except json.JSONDecodeError:
                        print("WARNING: " + str(file_name) + " is not a valid JSON file")
                    
                    file_id += 1

        return path_to_id, url_to_id
    
    def get_id(self, file_path : str) -> int:
        """
        Retrieves the assigned ID for a given file path

        :param file_path: File path to look up in dictionary
        :return: Unique integer ID if path found, else -1
        """

        return self.path_to_id.get(file_path, -1)

    def get_id_by_url(self, url: str) -> int:
        """
        Retrieves the assigned ID for a given URL

        :param url: URL to look up in dictionary
        :return: Unique integer ID if URL is found, else -1
        """

        return self.url_to_id.get(url, -1)

    def get_url_by_id(self, doc_id: int) -> str:
        """
        Retrieves the URL for a given document ID
        
        :param doc_id: Document ID to look up in dictionary
        :return: URL if found, else an empty string
        """

        return next((url for url, id in self.url_to_id.items() if id == doc_id), "")