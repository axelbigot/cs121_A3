import logging
import os
import json
import re

from index.defs import APP_DATA_DIR


_MAPPER_DIR = APP_DATA_DIR / 'mappers'

logger = logging.getLogger(__name__)

class PathMapper:
    """
    Class that scans JSON files in a specified directory and assigns each 
    file path a unique ID starting from 1 and incrementing
    """

    def __init__(self, root_path : str, *, rebuild = False):
        """
        Initializes the PathMapper with the dictionary containing JSON files
        Maps Path-to-ID and URL-to-ID

        :param doc_path: Path to the directory containing JSON files
        """
        self.root_path = root_path

        name = re.sub(r'[<>:"/\\|?*]', '_', self.root_path)
        self._mapper_disk_path = _MAPPER_DIR / name

        self.path_to_id, self.url_to_id = ({}, {})

        if rebuild or not self._load():
            logger.debug(f'Building PathMapper from scratch')
            self.construct_mapping()
            self._save()
    
    def construct_mapping(self):
        """
        Reads all JSON files in subdirectories of the root 
        directory and assigns each a unique ID

        :param: None
        :return: Dictionary mapping file paths to unique IDs
        """
        file_id = 1

        # walk through all subdirectories and files
        for subdir, _, files in os.walk(self.root_path):
            for file_name in files:
                if file_name.endswith(".json"):
                    file_path = os.path.join(subdir, file_name)

                    self.path_to_id[file_path] = file_id

                    # attempt to extract URL from JSON
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            url = data.get("url")

                            if url and url not in self.url_to_id:
                                self.url_to_id[url] = file_id
                    except json.JSONDecodeError:
                        print("WARNING: " + str(file_name) + " is not a valid JSON file")
                    
                    file_id += 1
    
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

    def _save(self):
        """
        Saves the mapper to disk, so it does not need to be rebuilt for later runs.
        """
        data = {
            'path_to_id': self.path_to_id,
            'url_to_id': self.url_to_id
        }

        self._mapper_disk_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self._mapper_disk_path, 'w') as f:
            json.dump(data, f)

        logger.debug(f'Saved PathMapper to {self._mapper_disk_path}')

    def _load(self) -> bool:
        """
        Load the path mapper from disk, if it already exists.

        Returns:
            True if loading succeeded, false otherwise.
        """
        if not self._mapper_disk_path.exists():
            logger.debug(f'Count not find existing PathMapper')
            return False

        logger.debug(f'Loading PathMapper from {self._mapper_disk_path}')

        with open(self._mapper_disk_path, 'r') as f:
            data = json.load(f)

        self.path_to_id = data["path_to_id"]
        self.url_to_id = data["url_to_id"]

        return True
