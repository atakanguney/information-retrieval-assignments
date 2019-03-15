from functools import reduce
import concurrent.futures
import os
import re
import json


def get_records(raw_text):
    # TODO: Write doc-string
    p = re.compile(r"<REUTERS(.*?)</REUTERS>", re.DOTALL)
    for match in p.finditer(raw_text):
        yield match.group(0)


def get_id(record):
    # TODO: Write doc-string
    p = re.compile(r'NEWID="(\d*?)"', re.DOTALL)
    match = p.search(record)
    if not match:
        print(record)
        raise ValueError("ID cannot be found")
    return match.group(1)


def get_tag_from_text(text, tag):
    """Get title between <'tag'> tags

    Parameters
    ==========
    text: str
        Text to extract 'tag'

    Returns
    =======
        str:
            Matched 'tag'
    """
    reg_pat = r"<{}>(.*?)</{}>".format(tag, tag)
    p = re.compile(reg_pat, re.DOTALL)
    match = p.search(text)
    if match:
        res = match.group(1)
    else:
        res = ""

    return res


def extract_id_document(record):
    """Get id, title and body

    Parameters
    ==========
    record: str
        Text to extract

    Returns
    =======
        str:
            id, title and body
    """
    id_ = get_id(record)
    text = get_tag_from_text(record, "TEXT").strip()
    title = get_tag_from_text(text, "TITLE").strip()
    body = get_tag_from_text(text, "BODY").strip()

    return int(id_), (title + " " + body).strip()


def read_input(path):
    """Read whole file

    Parameters
    ==========
    path: str
        Path to file

    Returns
    =======
        str:
            Raw Text
    """
    with open(path, "rb") as f:
        return f.read().decode("latin-1")


def read_punctuations(path):
    # TODO: Write doc-string    
    with open(path, "r") as f:
        return f.read().strip().split()


def read_stopwords(path):
    # TODO: Write doc-string
    with open(path, "r") as f:
        return f.read().strip().split()


class Tokenizer(object):
    """Tokenizer Class
    
    Attributes
    ==========
    punctuations: list
        Puntuations that are removed from the text
    stopwords: list
        Stopwords that are ignored when text is processed

    Parameters
    ==========
    punctuations_path: str
        Path to punctuations list
    stopwords_path: str
        Path to stopwords list
    """
    def __init__(self, punctuations_path, stopwords_path):
        self.punctuations = read_punctuations(punctuations_path)
        self.stopwords = read_stopwords(stopwords_path)

    def tokenize(self, text):
        """Tokenize given text

        Parameters
        ==========
        text: str
            Text to tokenize
        """
        # Replace puntuations with empty space
        for punc in self.punctuations:
            text = text.replace(punc, " ")

        # Case-Folding
        text = text.lower()

        # Remove stop-words
        return [
            word for word in text.strip().split()
            if word not in self.stopwords
        ]


def merge_indices(indices):
    # TODO: Write doc-string
    output = {}
    for index in indices:
        for key, value in index.items():
            output.setdefault(key, set({})).update(value)

    return output


class Indexer(object):
    # TODO: Write doc-string
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer

    def extract_tokens(self, path):
        # TODO: Write doc-string
        raw_text = read_input(path)
        records = get_records(raw_text)
        news_texts = dict([
            extract_id_document(record)
            for record in records
        ])
        return {
            id_: {
                *self.tokenizer.tokenize(news_text)
            }
            for id_, news_text in news_texts.items()
        }

    def construct_bigram_index(self, word):
        # TODO: Write doc-string
        bigram_index = {}
        word_ = "$" + word + "$"
        for i in range(0, len(word_) - 1):
            bigram_index.setdefault(word_[i:i+2], set()).add(word)
        return bigram_index

    def construct_indices_from_id_words(self, id_words):
        # TODO: Write doc-string
        inverted_index = {}
        bigram_indices = []
        
        for index, words in id_words.items():
            for word in words:
                if word:
                    inverted_index.setdefault(word, set()).add(index)
                    bigram_indices.append(self.construct_bigram_index(word))
        return inverted_index, merge_indices(bigram_indices)

    def helper(self, path):
        # TODO: Write doc-string
        id_words = self.extract_tokens(path)
        return self.construct_indices_from_id_words(id_words)

    def construct_all_indices(self, paths, parallelism=None):
        # TODO: Write doc-string
        if not parallelism:
            parallelism = os.cpu_count()
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as executor:
            inverted_bigram = list(executor.map(
                self.helper,
                paths
            ))

        inverted_indices = [
            tuple_[0]
            for tuple_ in inverted_bigram
        ]

        bigram_indices = [
            tuple_[1]
            for tuple_ in inverted_bigram
        ]

        return merge_indices(inverted_indices), merge_indices(bigram_indices)


def save_index(path, index):
    # TODO: Write doc-string
    with open(path, "w") as f:
        f.write(json.dumps(index))


def convert_set_to_list(dict_):
    # TODO: Write doc-string
    return {
        key: list(val)
        for key, val in dict_.items()
    }


if __name__ == "__main__":
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    script_parent = os.path.dirname(script_dir)

    punctuations_path = os.path.join(script_parent, "punctuations.txt")
    stopwords_path = os.path.join(script_parent, "stopwords.txt")
    base_folder = os.path.join(script_parent, "reuters21578")
    inverted_index_path = os.path.join(script_parent, "Output/index.json")
    bigrams_path = os.path.join(script_parent, "Output/bigrams.json")

    if not os.path.isdir(base_folder):
        raise ValueError("Folder not found: {}".format(base_folder))
    file_paths = [os.path.join(base_folder, "reut2-{:03d}.sgm".format(i)) for i in range(22)]

    tokenizer = Tokenizer(punctuations_path, stopwords_path)
    indexer = Indexer(tokenizer)
    inverted_index_set, bigrams_set = indexer.construct_all_indices(file_paths)
    save_index(inverted_index_path, convert_set_to_list(inverted_index_set))
    save_index(bigrams_path, convert_set_to_list(bigrams_set))
