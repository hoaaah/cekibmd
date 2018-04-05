import os

# (C) Copyright 2018 Heru Arief Wijaya (http://belajararief.com/) untuk INDONESIA.

def checkfile(filename):
    if os.path.exists(filename): 
        # execfile(filename)
        # exec(open(filename).read())
        open(filename).read()
    else:
        return 0