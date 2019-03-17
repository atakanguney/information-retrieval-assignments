Requirements
============
Python==3.6.6

There are 2 different modules under "Code" directory.

preprocessing.py
================
    python preprocessing.py
        It produces "index.json" and "bigram.json" under "Output" directory. Prints out
        answers to questions that are asked in the description to standard output

process.py
==========
    python process.py QUERY_TYPE QUERY
        It accepts 2 commandline arguments, if fewer provided it raises ValueError
        QUERY_TYPE must be integer

        Example:
        ========
            python process.py 1 "people AND car"
