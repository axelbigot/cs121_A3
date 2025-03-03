import re
import json
from bs4 import BeautifulSoup, NavigableString, Comment, Declaration
from textblob import Word

def _ignore_nested_tags(element):
    """
    Takes an element from HTML and removes all nested tags
    """
    for string in element:
        if isinstance(string, NavigableString) and not isinstance(string, Comment) and not isinstance(string, Declaration):
            yield string

def tokenize(string):
    """
    Returns an array of all the tokens in the given phrase.
    Runs in O(n) time where n is the number of words in the phrase

    Args:
       string: phrase to tokenize

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

def tokenize_JSON_file_with_tags(path, explicit_tags):
    """
    Tokenizes a JSON file but attaches the tag frequencies with each token.

    Args:
        path: str - path to JSON file
        explicit_tags - list[str] - list of tags that should be explicitly defined in the frequency dict
    Returns:
        dict: index is lemmatized tokens and value is dict[str, int] where index is HTML tag and value is frequency
    """
    dict_tags = explicit_tags + ["other"]
    
    with open(path, 'r') as file:
        obj = json.load(file) # convert json to dictionary
        soup = BeautifulSoup(obj['content'], 'html.parser')
        total_frequencies = compute_word_frequencies(Word(token).lemmatize() for token in tokenize(soup.get_text(' ')))

        # lemmatized token = {tag_frequencies}
        tag_frequencies = dict()

        # explicit tags:
        for tag in explicit_tags:
            for soup_tag in soup.find_all(tag):
                string = " ".join(_ignore_nested_tags(soup_tag))
                if string == None:
                    continue
                
                frequencies = compute_word_frequencies(Word(token).lemmatize() for token in tokenize(string))

                for token, frequency in frequencies.items():
                    frequency_dict = tag_frequencies.get(token, dict.fromkeys(dict_tags, 0))
                    frequency_dict[tag] = frequency_dict.get(tag) + frequency
                    
                    tag_frequencies[token] = frequency_dict
        
        # miscellaneous
        for token, total_frequency in total_frequencies.items():
            frequencies = tag_frequencies.get(token) or dict.fromkeys(dict_tags, 0)

            total = 0
            if frequencies != None:
                for frequency in frequencies.values():
                    total += frequency

            other_frequency = total_frequency - total
            frequencies["other"] = other_frequency

            if other_frequency < 0:
                raise Exception(f'Negative other score: {path}, {token}, {frequencies}')

            tag_frequencies[token] = frequencies

        return tag_frequencies
