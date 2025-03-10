import logging
import os

from openai import OpenAI


OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

logger = logging.getLogger(__name__)

class Summarizer():
    """
    A class to summarize the content of a file using OpenAI's GPT-4.

    Attributes:
        client (OpenAI): An instance of the OpenAI client to interact with the API.
    """

    def __init__(self):
        """
        Initializes the Summarizer class and sets up the OpenAI client.
        """
        if OPENAI_API_KEY:
            self.client = OpenAI()
        else:
            logger.warning('Missing OpenAPI Key. Summaries will not work (don\'t worry, the rest of the app will still run fine).')

    def getSummary(self, url):
        """
        Reads the content of a file from the specified URL, sends it to the OpenAI API, and returns the summary.

        Args:
            url (str): The path to the file that needs to be summarized.

        Returns:
            str: The summarized content generated by OpenAI's GPT-4 model.
        
        Raises:
            FileNotFoundError: If the file at the provided path does not exist.
            Exception: If there is an issue with the OpenAI API call.
        """
        if not OPENAI_API_KEY:
            return "You haven't set the OPENAI_API_KEY environment variable. AI summaries are disabled!"
        
        # Open the file in read mode
        with open(url, 'r') as file:
            content = file.read()
        
        # Send the content to the OpenAI API for summarization
        try:
            completion = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are website summarizer. Answer with plain text. No font bolding."},
                    {
                        "role": "user",
                        "content": content
                    }
                ]
            )
            # Return the summary from the API response
            return completion.choices[0].message.content
        
        except Exception as e:
            # Handle any errors that occur during the API call
            raise Exception(f"Error with OpenAI API call: {str(e)}")