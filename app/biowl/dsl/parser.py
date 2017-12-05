from sys import stdin, stdout, stderr, argv, exit
import os
import json
import sys

import code

from pyparsing import *

from .grammar import PythonGrammar
from .context import Context
from .interpreter import Interpreter
from .pygen import CodeGenerator
 
class PhenoWLParser(object):
    '''
    The parser for PhenoWL DSL.
    '''

    def __init__(self, grammar = None):
        self.grammar = grammar if grammar else PhenoWLGrammar()
        self.tokens = ParseResults()
        self.err = []
    
    def error(self, *args):
        self.err.append("{0}".format(', '.join(map(str, args))))

    def parse(self, text):
        try:
            self.tokens = self.grammar.program.ignore(pythonStyleComment).parseString(text, parseAll=True)
            return self.tokens
        except ParseException as err:
            print(err)
            self.error(err)
        except Exception as err:
            print(err)
            self.error(err)
    
    def parse_subgrammar(self, subgrammer, text):
        try:
            self.tokens = subgrammer.ignore(pythonStyleComment).parseString(text, parseAll=True)
            return self.tokens
        except ParseException as err:
            print(err)
            self.error(err)
        except Exception as err:
            print(err)
            self.error(err)

    def parse_file(self, filename):
        try:
            self.tokens = self.grammar.program.ignore(pythonStyleComment).parseFile(filename, parseAll=True)
            return self.tokens
        except ParseException as err:
            print(err)
            exit(3)
        except Exception as err:
            print(err)
            self.error(err)
        
if __name__ == "__main__":
    from ..timer import Timer
    with Timer() as t:
        p = PhenoWLParser(PythonGrammar())
        if len(sys.argv) > 1:
            tokens = p.parse_file(sys.argv[1])
        else:
            test_program_example = """
#shippi.RegisterImage('127.0.0.1', 'phenodoop', 'sr-hadoop', '/home/phenodoop/phenowl/storage/images', '/home/phenodoop/phenowl/storage/output')
# GetFolders('/')
# CreateFolder('/images/img')           
# x = 10
# y = 10
# z = 30
# for k in range(1,10):
#     p =30
#     q = 40
#     if x <= 20:
#         r = 40
#         s = 50
#         if y >= 10:
#             t = 60
#             s = 70
#             print(z)
# if p < q:
#     print(p + 5)

# task sparktest(s, u, p):
#     GetTools()
# sparktest('server', 'user', 'password')

# parallel:
#     x = 10
#     q = x
#     print(q)
# with:
#     y = 20
#     p = y
#     print(p)
    
# task ('http://sr-p2irc-big8.usask.ca:8080', '7483fa940d53add053903042c39f853a'):
#     ws = GetHistoryIDs()
#     print(len(ws))
#     l = len(ws)
#     if l > 0:
#         print(ws[0])
#         w = GetHistory(ws[0])
#         r = Upload(w['id'], '/home/phenodoop/phenowl/storage/texts/test.txt')
#         print(r)
        #print(w)
        #print(len(w))
        #print(w)
        #print(w['name'])

#result = SearchEntrez("Myb AND txid3702[ORGN] AND 0:6000[SLEN]", "nucleotide")
#print(result)

# s = 10
# t = "15" + "16"
# print(t)

# task ('http://sr-p2irc-big8.usask.ca:8080', '7483fa940d53add053903042c39f853a'):
#     history_id = CreateHistory('Galaxy Pipeline')
#     dataset_id = FtpToHistory('ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR034/SRR034608/SRR034608.fastq.gz', history_id)
#     tool_id = ToolNameToID('FASTQ Groomer')
#     ref_dataset_id = HttpToHistory('http://rice.plantbiology.msu.edu/pub/data/Eukaryotic_Projects/o_sativa/annotation_dbs/pseudomolecules/version_6.1/all.dir/all.cDNA.gz', history_id)
#     params = "name:" + ref_dataset_id
#     r = RunTool(history_id, tool_id, params)
#     
#     output = r['name']
#     print(output)



#x[0] = 5*4
#z = x[0] 
#y = 50 + z
# a = {3: {'t':'ss'}, 4:11}
# y = a[3]
# x = []
# x[0] = 20
# y = 5 + (x[0])
# print(y)

# f = FastQC('fastq\SRR034608.fastq.gz')
# print(f)

# parallel:
#     print(10)
# with:
#     print(11)

ref_dataset_name = 100
#params = [ref_dataset_name, 50]
# { 'name': ref_dataset_name }
#params = [{ 'name': ref_dataset_name }, { 'name': [20 * 30] }] 
#params = { 'name': ref_dataset_name }

    params = {"fastq_R1": {"values":[{"src":"hda", "id":"9ac6c47a8515c831"}]}, "fastq_R2":{"values":[{"src":"hda","id":"6dbc21d257b88b00"}]}}
    
print(params = 'xt')

            """
        tokens = p.parse(test_program_example)
        #tokens = p.grammar.assignstmt.ignore(pythonStyleComment).parseString(test_program_example)
            
        tokens.pprint()
        #print(tokens.asXML())
        integrator = PhenoWLInterpreter()
       # integrator = PhenoWLCodeGenerator()
        
        integrator.context.load_library("libraries")
        integrator.run(tokens)
    print(integrator.context.library)
    print(integrator.context.out)
    print(integrator.context.err)
    #print(integrator.code)
