from typing import TextIO
import pyparsing

class PsqlParser:

    debug: bool == True
    fout: TextIO
    
    def __init__(self):
        if self.debug:
            self.fout = open('psqlparser.log', 'w')

    

    
