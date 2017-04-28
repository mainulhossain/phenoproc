# grammar.py
#
# PhenoWL grammar   
#
#
from pip._vendor.pyparsing import delimitedList
from token import RPAR, NEWLINE
"""
    Terminals are in quotes, () is used for bracketing.

    program:    decl*

    decl:   vardecl
            fundecl

    vardecl: NAME;
             NAME = expr;

    fundecl:    NAME "(" args ")" "{" body "}"

    args:        /*empty*/
            ( arg "," )* arg

    arg:        NAME
                NAME=expr

    body:        vardecl* stmt*

    stmt:        ifstmt
            whilestmt
            "return" expr ";"
            expr ";"
            "{" stmt* "}"
            ";"

    ifstmt:        "if" "(" expr ")" stmt
            "if" "(" expr ")" stmt "else" stmt

    whilestmt:    "while" "(" expr ")" stmt

    expr:        expr binop expr
            unop expr
            expr "[" expr "]"
            "(" expr ")"
            expr "(" exprs ")"
            NAME
            INT
            CHAR
            STRING

    exprs:        /*empty*/
            (expr ",")* expr

    binop:        "+" | "-" | "*" | "/" | "%" |
            "=" |
            "<" | "==" | "!="

    unop:        "!" | "-" | "*"

    type:        "int" stars
            "char" stars

    stars:        "*"*
"""

from pyparsing import *


class Enumerate(dict):
    """C enum emulation (original by Scott David Daniels)"""
    def __init__(self, names):
        for number, name in enumerate(names.split()):
            setattr(self, name, number)
            self[number] = name

class SharedData(object):
    """Data used in all three main classes"""

    #Possible kinds of symbol table entries
    KINDS = Enumerate("NO_KIND WORKING_REGISTER GLOBAL_VAR FUNCTION PARAMETER LOCAL_VAR CONSTANT")
    #Supported types of functions and variables
    TYPES = Enumerate("NO_TYPE INT UNSIGNED")

    #bit size of variables
    TYPE_BIT_SIZE = 16
    #min/max values of constants
    MIN_INT = -2 ** (TYPE_BIT_SIZE - 1)
    MAX_INT = 2 ** (TYPE_BIT_SIZE - 1) - 1
    MAX_UNSIGNED = 2 ** TYPE_BIT_SIZE - 1
    #available working registers (the last one is the register for function's return value!)
    REGISTERS = "%0 %1 %2 %3 %4 %5 %6 %7 %8 %9 %10 %11 %12 %13".split()
    #register for function's return value
    FUNCTION_REGISTER = len(REGISTERS) - 1
    #the index of last working register
    LAST_WORKING_REGISTER = len(REGISTERS) - 2
    #list of relational operators
    RELATIONAL_OPERATORS = "< > <= >= == !=".split()

    def __init__(self):
        #index of the currently parsed function
        self.functon_index = 0
        #name of the currently parsed function
        self.functon_name = 0
        #number of parameters of the currently parsed function
        self.function_params = 0
        #number of local variables of the currently parsed function
        self.function_vars = 0


def cmdCallAction(s, l, t):
    print("cmd call found:", t)

def moduleCallAction(s, l, t):
    print("mod call found:", t)

def varDeclAction(s, l, t):
    print("varDeclAction found:", t)

def funcDeclAction(s, l, t):
    print("funcDeclAction found:", t)
    
def funcCallAction(s, l, t):
    print("funcCallAction found:", t)
    
def exprAction(s, l, t):
    print("exprAction found:", t)
       
def taskDeclAction(s, l, t):
    print("task found:", t)
    
def stmtAction(s, l, t):
    print("stmt found:", t)
                
def ifstmtAction(s, l, t):
    print("ifstmt found:", t)
def whilestmtAction(s, l, t):
    print("whilestmt found:", t)
def returnstmtAction(s, l, t):
    print("returnstmt found:", t)
def argAction(s, l, t):
    print("argAction found:", t)
def keyargAction(s, l, t):
    print("keyargAction found:", t)
def varBodyAction(s, l, t):
    print("body found:", t)

def optAction(s, l, t):
    print("optAction", t)
    
def mulexp_action(s, l, t):
    print("mulexp", t)
def numexp_action(s, l, t):
    print("numexp", t)
                
LPAR,RPAR,LBRACE,RBRACE,SEMI,COMMA = map(Suppress, "(){};,")
LBRACK,RBRACK = map(Literal, "[]")
INT = Keyword("int")
CHAR = Keyword("char")
WHILE = Keyword("while")
DO = Keyword("do")
IF = Keyword("if")
ELSE = Keyword("else")
RETURN = Keyword("return")
INT = Keyword("int")
CHAR = Keyword("char")
TASK = Keyword("task")
#CMD = Keyword("cmd")
SYNC = Keyword("sync")
TRUE = Keyword("True")
FALSE = Keyword("False")

NAME = Word(alphas+"_", alphanums+"_")
integer = Regex(r"[+-]?\d+")
char = Regex(r"'.'")
string_ = dblQuotedString
ASSIGN = Literal('=')

TYPE = Group((INT | CHAR) + ZeroOrMore("*"))

expr = Forward()
operand = NAME | integer | char | string_

callarg = Group(expr + FollowedBy(COMMA | RPAR)).setParseAction(argAction)
#callarg = Group(expr)
taskdecl = Forward()
funccall = Group((NAME + FollowedBy("(")) + Suppress("(") + Optional(Group(delimitedList(callarg)))("callarg") + Suppress(")")).setParseAction(funcCallAction)
        
expr <<= (funccall | operatorPrecedence(operand, 
    [
    (oneOf('! - *'), 1, opAssoc.RIGHT),
    (oneOf('* / %'), 2, opAssoc.LEFT),
    (oneOf('+ -'), 2, opAssoc.LEFT),
    (oneOf('< == > <= >= !='), 2, opAssoc.LEFT),
    (Regex(r'=[^=]'), 2, opAssoc.LEFT),
    ]) + 
         Optional(LBRACK + expr + RBRACK | LPAR + Group(Optional(delimitedList(expr))) + RPAR )
    ).setParseAction(exprAction)

stmt = Forward()

ifstmt = (IF - LPAR + expr + RPAR + stmt + Optional(ELSE + stmt)).setParseAction(ifstmtAction)
whilestmt = (WHILE - LPAR + expr + RPAR + stmt).setParseAction(whilestmtAction)
returnstmt = (RETURN - expr).setParseAction(returnstmtAction)

stmt << Group( ifstmt |
          whilestmt |
          returnstmt | 
          expr |
          LBRACE + ZeroOrMore(stmt) + RBRACE).setParseAction(stmtAction)

vardecl = (Group(NAME + ASSIGN + (expr | "[]"))).setParseAction(varDeclAction) # + Optional(ASSIGN + (operand | "[]"))

key_arg = Group(NAME + ASSIGN + expr + FollowedBy(COMMA | RPAR)).setParseAction(keyargAction)

body = (ZeroOrMore(vardecl | taskdecl | stmt)).setParseAction(varBodyAction)

#positional_arguments = (NAME + ZeroOrMore(COMMA + NAME)).setParseAction(posargAction)
#keyword_item = Group(NAME + Optional(ASSIGN + expr))
#keyword_arguments = keyword_item + ZeroOrMore(COMMA + keyword_item)
#argument_list = positional_arguments + Optional(COMMA + keyword_arguments)

#signature = LPAR + Optional(Group(keyword_arguments)) + RPAR + LBRACE + Group(body) + RBRACE
#fundecl = Group(NAME + signature).setParseAction(funcDeclAction)
arg = Group(NAME + FollowedBy(COMMA | RPAR)).setParseAction(argAction)
fundecl = Group(NAME + LPAR + Optional(Group(delimitedList(arg | key_arg))) + RPAR +
            LBRACE + Group(body) + RBRACE).setParseAction(funcDeclAction)
taskdecl <<= Group(TASK + NAME + LPAR + Optional(Group(delimitedList(arg | key_arg))) + RPAR +
            LBRACE + Group(body) + RBRACE).setParseAction(taskDeclAction)
decl = taskdecl | fundecl | vardecl
program = ZeroOrMore(decl)

program.ignore(cStyleComment)

# set parser element names
for vname in ("ifstmt whilestmt returnstmt "
               "NAME fundecl vardecl program arg body stmt".split()):
    v = vars()[vname]
    v.setName(vname)

#~ for vname in "fundecl stmt".split():
    #~ v = vars()[vname]
    #~ v.setDebug()

test = r"""
task main()
{
    i = 10
    j = i + 20
    while(t > 0)
    {
    }
    task xx()
    {
    }
    task x3()
    {
        task x3_1()
        {
        }
    }
}


"""

ast = program.parseString(test,parseAll=True)

import pprint
#pprint.pprint(ast.asList())