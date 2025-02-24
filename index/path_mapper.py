import os
import json

class PathMapper:
    """
    Class that scans JSON files in a specified directory and assigns each 
    file path a unique ID starting from 1 and incrementing
    """

    def __init__(self, root_path : str):
        """
        Initializes the PathMapper with the directionary containing JSON files
        Maps Path-to-ID

        :param doc_path: Path to the directory containing JSON files
        """

        self.root_path = root_path
        self.path_to_id = self.construct_mapping()
    
    def construct_mapping(self) -> dict[str, int]:
        """
        Reads all JSON files in subdirectories of the root 
        directory and assigns each a unique ID

        :param: None
        :return: Dictionary mapping file paths to unique IDs
        """

        mapping = {}
        file_id = 1

        # walk through all subdirectories and files
        for subdir, _, files in os.walk(self.root_path):
            for file_name in files:
                if file_name.endswith(".json"):
                    file_path = os.path.join(subdir, file_name)

                    mapping[file_path] = file_id
                    file_id += 1
                    

                    # url mapping implementation
                    """
                    with open(file_path, "r", encoding="utf-8") as f:
                        try:
                            data = json.load(f)
                            url = data.get("url")

                            if url and url not in mapping:
                                mapping[url] = url_id
                                url_id += 1

                        except json.JSONDecodeError:
                            print("WARNING: " + str(file_name) + " is not a valid JSON file")
                    """

        return mapping
    
    def get_id(self, file_path : str) -> int:
        """
        Retrives the assigned ID for a given file path

        :param file_path: File path to look up in dictionary
        :return: Unique integer ID if path found, else -1
        """

        return self.path_to_id.get(file_path, -1)