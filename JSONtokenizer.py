import re
import json
from bs4 import BeautifulSoup
import os
from textblob import Word

def tokenize(string):
    """
    Returns an array of all the tokens in the given file.
    Runs in O(n) time where n is the number of words in the file. stop_words is a set
    so looking if an element exists take O(1) time.

    Args:
        path(string): path to file

    Returns:
        generator: generates list of tokens
    """

    return (word.lower() for word in re.sub(r'[^a-zA-Z\d]', " ", string).split() if len(word) > 1)

def compute_word_frequencies(tokens):
    """
    Calculates the frequency of each word in the given array.
    Runs in O(n) time where n is the number of words in the list, assuming the time complexity
    of dictionary.get() is O(1). Worse case, dictionary.get() runs in O(n) time
    which makes this function run in O(n^2).

    Args:
        tokens(list): list of words

    Returns:
        dict: Dictionary with the word as the index and the count as the value
    """
    countMap = {}
    
    for token in tokens:
        countMap[token] = countMap.get(token, 0) + 1

    return countMap

def tokenize_JSON_file(path):
    """
    Tokenizes the content in the JSON file. JSON file must have 1 object which should have the properties
    url, content, encoding.

    Args:
        path: str - path to JSON file

    Returns:
        generator: generates list of tokens in json.content property
    """
    with open(path, 'r') as file:
        obj = json.load(file) # convert json to dictionary
        soup = BeautifulSoup(obj['content'], 'html.parser')
        
        return (Word(token).lemmatize() for token in tokenize(soup.get_text()))

if __name__ == '__main__':
    for dirname in os.listdir('DEV'):
        dirname1 = os.path.join('DEV', dirname)
        for filename in os.listdir(dirname1):
            print(list(tokenize_JSON_file(os.path.join(dirname1, filename))))
