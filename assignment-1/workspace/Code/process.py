import json
import os
import re
import sys
from functools import reduce


class Processor(object):
    """Boolean Processor Class

    Attributes
    ==========
    inverted_index: dict
        Index for terms
    bigram_index: dict
        Index for bigrams

    Parameters
    ==========
    inverted_index_path: str
        Path to read inverted index
    bigrams_index_path: str
        Path to read bigrams index
    """
    def __init__(self, inverted_index_path, bigrams_index_path):
        with open(inverted_index_path, "r") as fp:
            self.inverted_index = json.load(fp)
        with open(bigrams_index_path, "r") as fp:
            self.bigram_index = json.load(fp)

    def get_bigrams(self, begin, end):
        """Get bigrams for wildcard query

        It splits query string by '*' and constructs
        bigrams for beginning of query and ending of query

        Parameters
        ==========
        begin: str
            String that shoul match with beginning
        end: str
            String that shoul match with ending
        Returns
        =======
            list:
                List of bigrams
        """
        begin_, end_ = "$" + begin, end + "$"
        bigrams_begin = [
            begin_[i:i+2] for i in range(0, len(begin_) - 1)
        ]

        bigrams_end = [
            end_[i:i+2] for i in range(0, len(end_) - 1)
        ]

        return bigrams_begin + bigrams_end

    def preprocess_query(self, query):
        """Preprocess query string

        It determines query type, and according to type
        produces bigram or keyword list

        Parameters
        ==========
        query: str
            Query string

        Returns
        =======
            str:
                Preprocessed query
        """
        query = query.strip().casefold()
        return query

    def get_keywords(self, query):
        """Get keywords from the query

        Parameters
        ==========
        query: str
            Query string

        Returns
        =======
            list:
                List of keywords to search
        """
        separators = ["and", "or"]

        keywords = [keyword for keyword in query.split() if keyword not in separators]
        return keywords

    def get_matched_terms(self, bigrams, begin, end):
        """Get matched terms

        It gets terms from bigram index for wildcard queries and
        applies post filtering to get rid of false positives

        Parameters
        ==========
        bigrams: list
            List of bigrams
        begin: str
            String that shoul match with beginning
        end: str
            String that shoul match with ending
        Returns
        =======
            list:
                List of terms that are matched with bigrams and
                they are 'POST-FILTERED'
        """
        begin_, end_ = "^" + begin + ".*", ".*" + end + "$"
        def postprocess(term):
            if re.match(begin_, term) and re.match(end_, term):
                return True
            else:
                return False

        terms = {
            term
            for bigram in bigrams
            for term in self.bigram_index.get(bigram, [])
            if postprocess(term)
        }

        return list(terms)

    def get_matched_documents(self, terms, type_):
        """Get matched documents from inverted index

        Parameters
        ==========
        terms: list
            List of terms to be searched in inverted index
        type_: str
            Type of query

        Returns
        =======
            list:
                List of unique document ids for qiven terms
        """
        matched_documents_list = [set(self.inverted_index.get(term, [])) for term in terms]
        if type_ == "conjunctive":
            if matched_documents_list:
                return list(reduce(set.intersection, matched_documents_list))
            else:
                return []
        else:   
            return list(reduce(set.union, matched_documents_list, set()))

    def search_qeury(self, query, query_type):
        """Search for query string

        Parameters
        ==========
        query: str
            Query string
        query_type: str
            Query type

        Returns
        =======
            list:
                Sorted list of matched document ids
        """
        if not query:
            return []
        query = self.preprocess_query(query)
        if query_type == "wildcard":
            if "*" not in query:
                terms = self.get_keywords(query)
            else:
                begin, end = query.split("*")
                bigrams = self.get_bigrams(begin, end)
                terms = self.get_matched_terms(bigrams, begin, end)
        else:
            terms = self.get_keywords(query)

        return sorted(self.get_matched_documents(terms, query_type))


if __name__ == "__main__":
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    script_parent = os.path.dirname(script_dir)

    inverted_index_path = os.path.join(script_parent, "Output/index.json")
    bigrams_path = os.path.join(script_parent, "Output/bigrams.json")

    query_types = {
        1: "conjunctive",
        2: "disjunctive",
        3: "wildcard"
    }
    processor = Processor(inverted_index_path, bigrams_path)

    args = sys.argv
    try:
        query_type = int(args[1])
        query_type = query_types[query_type]
    except ValueError as ve:
        raise ValueError("Query type must be integer: {}".format(args[1]))
    except KeyError as ke:
        raise KeyError("Unsupported query type: {}".format(args[1]))
    
    query_str = args[2]
    print(processor.search_qeury(query_str, query_type))
