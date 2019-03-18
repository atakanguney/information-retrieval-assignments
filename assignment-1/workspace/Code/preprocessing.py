import json
import os
import re
from collections import Counter


class Reader(object):
    """Reader class to read data"""

    @staticmethod
    def get_records(raw_text):
        """Get records from raw text

        Parameters
        ----------
        raw_text: str
            Raw text data read from file

        Yields
        ------
        str:
            Record
        """
        p = re.compile(r"<REUTERS(.*?)</REUTERS>", re.DOTALL)
        for match in p.finditer(raw_text):
            yield match.group(0)

    @staticmethod
    def get_id(record):
        """Get id of a record

        Parameters
        ----------
        record: str

        Returns
        -------
        int:
            id of the record
        """
        p = re.compile(r'NEWID="(\d*?)"')
        match = p.search(record)
        if not match:
            raise ValueError("ID cannot be found in record: {}".format(record))
        return int(match.group(1))

    @staticmethod
    def get_tag_from_text(text, tag):
        """Get title between <'tag'> tags

        Parameters
        ----------
        text: str
            Text to extract 'tag'

        Returns
        -------
        str:
            Matched 'tag'
        """
        reg_pat = r"<{}.*?>(.*?)</{}>".format(tag, tag)
        p = re.compile(reg_pat, re.DOTALL)
        match = p.search(text)
        if match:
            res = match.group(1)
        else:
            res = ""

        return res

    @staticmethod
    def read_input(path):
        """Read whole file

        Parameters
        ----------
        path: str
            Path to file

        Returns
        -------
        str:
            Raw Text
        """
        with open(path, "rb") as f:
            return f.read().decode("latin-1")

    @classmethod
    def extract_id_document(cls, record):
        """Get id, title and body

        Parameters
        ----------
        record: str
            Text to extract

        Returns
        -------
        str:
            id, title and body
        """
        id_ = cls.get_id(record)
        text = cls.get_tag_from_text(record, "TEXT").strip()
        title = cls.get_tag_from_text(text, "TITLE").strip()
        body = cls.get_tag_from_text(text, "BODY").strip()

        return id_, (title + " " + body).strip()

    @classmethod
    def get_all_id_text(cls, paths):
        id_text = dict([
            cls.extract_id_document(record)
            for path in paths
            for record in cls.get_records(cls.read_input(path))
        ])

        return id_text


class Tokenizer(object):
    """Tokenizer Class
    
    Attributes
    ----------
    punctuations: list
        Puntuations that are removed from the text
    stopwords: list
        Stopwords that are ignored when text is processed

    Parameters
    ----------
    punctuations_path: str
        Path to punctuations list
    stopwords_path: str
        Path to stopwords list
    """
    @staticmethod
    def read_punctuations(path):
        """Read punctuations from file

        Parameters
        ----------
        path: str
            Path to punctuations list
    
        Returns
        -------
        list:
            List of punctuations
        """    
        with open(path, "r") as f:
            return f.read().strip().split()

    @staticmethod
    def read_stopwords(path):
        """Read stopwords

        Parameters
        ----------
        path: str
            Path to stopwords list

        Returns
        -------
        list:
            List of stopwords
        """
        with open(path, "r") as f:
            return f.read().strip().split()

    @staticmethod
    def get_number_of_tokens(id_tokens):
        """Get number of tokens from id_tokens dict

        Parameters
        ----------
        id_tokens: dict
            Dict of id: list of tokens

        Returns
        -------
        int:
            Number of tokens in corpus
        """
        all_tokens = []
        for tokens in id_tokens.values():
            all_tokens.extend(tokens)
        return len(all_tokens)

    @staticmethod
    def get_number_of_terms_and_top_20(id_tokens):
        """Get number of terms and top 20 frequent token

        Parameters
        ----------
        id_tokens: dict
            Dict of id: list of tokens

        Returns
        -------
        tuple:
            Number of unique tokens in the corpus and top 20 frequent tokens
        """
        all_terms = []
        for tokens in id_tokens.values():
            all_terms.extend(tokens)
        top_20_terms = Counter(all_terms).most_common(20)
        all_terms = set(all_terms)
        return len(all_terms), top_20_terms

    def __init__(self, punctuations_path, stopwords_path):
        self.punctuations = self.read_punctuations(punctuations_path)
        self.stopwords = self.read_stopwords(stopwords_path)

    def replace_punctuations(self, text):
        """Replace punctuations for space for given text

        Parameters
        ----------
        text: str
            String to replace punctuations

        Returns
        -------
        str:
            String that punctuations are replaced with space
        """
        for punc in self.punctuations:
            text = text.replace(punc, " ")
        return text

    def remove_stopwords(self, words):
        """Remove stopwords from the list of words

        Parameters
        ----------
        words: list
            List of words

        Returns
        -------
        list:
            List of words that stopwords are excluded
        """
        return [
            word for word in words
            if word not in self.stopwords
        ]

    def split_tokens(self, text):
        """Split text into tokens by space

        Parameters
        ----------
        text: str
            Text to be splitted

        Returns
        -------
        list:
            List of tokens
        """
        return text.strip().split()

    def replace_punctuations_and_split_tokens(self, text):
        """Replace punctuations and split tokens

        Parameters
        ----------
        text: str
            Text to replace punctuations and split

        Returns
        -------
        list:
            List of tokens after punctuations replaced by space
            and splitted by space
        """
        # Replace puntuations with empty space
        text = self.replace_punctuations(text)

        # Split by space
        tokens = self.split_tokens(text)

        return tokens

    def casefolding(self, tokens):
        """Apply case folding to every token in given token list

        Parameters
        ----------
        tokens: list
            List of tokens

        Returns
        -------
        list:
            List of tokens that each token is applied casefolding
        """
        return [word.casefold() for word in tokens]

    def tokenize(self, text):
        """Tokenize given text

        Parameters
        ----------
        text: str
            Text to tokenize

        Returns
        -------
        list:
            List of tokens after tokenization procedure is applied
        """
        # Replace punctuations and split tokens
        tokens = self.replace_punctuations_and_split_tokens(text)

        # Case-Folding
        tokens = self.casefolding(tokens)

        # Remove stopwords
        tokens = self.remove_stopwords(tokens)

        return tokens


class Indexer(object):
    """Indexer Class"""

    @staticmethod
    def merge_indices(indices):
        """Merge indices

        Merges list of indices that are dict

        Parameters
        ----------
        indices: list of index

        Returns
        -------
        dict:
            Merged indices
        """
        output = {}
        for index in indices:
            for key, value in index.items():
                output.setdefault(key, set({})).update(value)

        return output

    def extract_bigram_index(self, word):
        """Extract bigram index of giver word

        A dictionary is constructed by bigrams generated from the given word

        Parameters
        ----------
        word: str
            Word to extract bigram index

        Returns
        -------
        dict:
            Index for given word
        """
        bigram_index = {}
        word_ = "$" + word + "$"
        for i in range(0, len(word_) - 1):
            bigram_index.setdefault(word_[i:i+2], set()).add(word)
        return bigram_index

    def construct_indices_from_id_terms(self, id_terms):
        """Construct inverted index and bigram index

        Parameters
        ----------
        id_terms: dict
            Dict of id: terms

        Returns
        -------
        tuple:
            Inverted index and Bigram index
        """
        inverted_index = {}
        bigram_indices = []
        
        for index, terms in id_terms.items():
            for term in terms:
                if term:
                    inverted_index.setdefault(term, set()).add(index)
                    bigram_indices.append(self.extract_bigram_index(term))

        return inverted_index, self.merge_indices(bigram_indices)

    def get_id_terms(self, id_tokens):
        """Get id: terms pairs for given id: tokens pairs

        Parameters
        ----------
        id_tokens: dict
            Dictionary of id: tokens

        Returns
        -------
        dict:
            Dictionary of id: terms
        """
        return {
            id_: set(tokens)
            for id_, tokens in id_tokens.items()
        }

    def construct_indices(self, id_tokens):
        """Construct indices from given id: tokens dict

        Parameters
        ----------
        id_tokens: dict
            Dictionary of id: tokens

        Returns
        -------
        tuple:
            Inverted index and Bigram index
        """
        id_terms = self.get_id_terms(id_tokens)
        return self.construct_indices_from_id_terms(id_terms)

    @staticmethod
    def save_index(path, index):
        """Save index to given path

        Parameters
        ----------
        path: str
            Path to write given index
        index: dict
            Index to save
        """
        with open(path, "w") as f:
            f.write(json.dumps(index))

    @staticmethod
    def convert_set_to_list(dict_):
        """Convert set valued dict to list valued dict

        Parameters
        ----------
        dict_: dict
            Dictionary to be converted

        Returns
        -------
        dict:
            Converted dictionary
        """
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

    reader = Reader()
    id_text = reader.get_all_id_text(file_paths)

    tokenizer = Tokenizer(punctuations_path, stopwords_path)
    # Replace punctuations and split by space
    id_tokens = {
        id_: tokenizer.replace_punctuations_and_split_tokens(text)
        for id_, text in id_text.items()
    }
    number_of_terms, top_20_terms = tokenizer.get_number_of_terms_and_top_20(id_tokens)
    print("Number of terms before stopword removal and casefolding: {}".format(number_of_terms))
    print("Top 20 terms before stopword removal and casefolding:")
    for term, freq in top_20_terms:
        print("Term: {}, Frequency: {}".format(term, freq))

    # Case folding
    id_tokens = {
        id_: tokenizer.casefolding(tokens)
        for id_, tokens in id_tokens.items()
    }
    number_of_tokens = tokenizer.get_number_of_tokens(id_tokens)
    print("Number of tokens before stopword removal: {}".format(number_of_tokens))
    # Stopword removal
    id_tokens = {
        id_: tokenizer.remove_stopwords(tokens)
        for id_, tokens in id_tokens.items()
    }
    number_of_tokens = tokenizer.get_number_of_tokens(id_tokens)
    print("Number of tokens after stopword removal: {}".format(number_of_tokens))
    number_of_terms, top_20_terms = tokenizer.get_number_of_terms_and_top_20(id_tokens)
    print("Number of terms after stopword removal and casefolding: {}".format(number_of_terms))
    print("Top 20 terms after stopword removal and casefolding:")
    for term, freq in top_20_terms:
        print("Term: {}, Frequency: {}".format(term, freq))


    indexer = Indexer()
    inverted_index, bigram_index = indexer.construct_indices(id_tokens)
    inverted_index = Indexer.convert_set_to_list(inverted_index)
    bigram_index = Indexer.convert_set_to_list(bigram_index)
    Indexer.save_index(inverted_index_path, inverted_index)
    Indexer.save_index(bigrams_path, bigram_index)
