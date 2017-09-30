from pyparsing import *
from pyparsing import _bslash
from sys import stdin, stdout, stderr, argv, exit
import os
import json
import sys
import ast
import threading
import _thread
import logging
import code

from .timer import Timer
from .func_resolver import Library
from .tasks import TaskManager

logging.basicConfig(level=logging.DEBUG)


def myIndentedBlock(blockStatementExpr, indentStack, indent=True):
    '''
    Modifies the pyparsing indentedBlock to build the AST correctly
    '''
    def checkPeerIndent(s,l,t):
        if l >= len(s): return
        curCol = col(l,s)
        if curCol != indentStack[-1]:
            if curCol > indentStack[-1]:
                raise ParseFatalException(s,l,"illegal nesting")
            raise ParseException(s,l,"not a peer entry")

    def checkSubIndent(s,l,t):
        curCol = col(l,s)
        if curCol > indentStack[-1]:
            indentStack.append( curCol )
        else:
            raise ParseException(s,l,"not a subentry")

    def checkUnindent(s,l,t):
        if l >= len(s): return
        curCol = col(l,s)
        if not(indentStack and curCol < indentStack[-1] and curCol <= indentStack[-2]):
            raise ParseException(s,l,"not an unindent")
#        indentStack.pop()

    NL = OneOrMore(LineEnd().setWhitespaceChars("\t ").suppress())
    INDENT = (Empty() + Empty().setParseAction(checkSubIndent)).setName('INDENT')
    PEER   = Empty().setParseAction(checkPeerIndent).setName('')
    UNDENT = Empty().setParseAction(checkUnindent).setName('UNINDENT')
    if indent:
        smExpr = Group( Optional(NL) +
            #~ FollowedBy(blockStatementExpr) +
            INDENT + (OneOrMore( PEER + Group(blockStatementExpr) + Optional(NL) )) + UNDENT)
    else:
        smExpr = Group( Optional(NL) +
            (OneOrMore( PEER + Group(blockStatementExpr) + Optional(NL) )) )
    blockStatementExpr.ignore(_bslash + LineEnd())
    return smExpr.setName('indented block')

class SymbolTable():
    '''
    Table to hold program symbols (vars and functions).
    For local symbols, a stack of symbol tables is
    maintained.
    '''
    def __init__(self):
        '''
        Initializes the table.
        '''
        self.vars = {}
        self.funcs = {}
        
    def add_var(self, name, value):
        '''
        Adds/Overwrites a variable to the symbol table.
        :param name:
        :param value:
        '''
        self.vars[name] = value
    
    def update_var(self, name, value):
        '''
        Updates a variable in the symbol table
        :param name:
        :param value:
        '''
        self.check_var(name)
        self.vars[name] = value
    
    def var_exists(self, name):
        '''
        Checks if a variable exists in the symbol table
        :param name:
        '''
        return name in self.vars
    
    def check_var(self, name):
        if not self.var_exists(name):
            raise "var {0} does not exist".format(name)
        return True
            
    def add_func(self, module, internal_name, func_name, args):
        '''
        Add a new function in the symbol table. The function table is a dictionary with key-value pair as:
        [ <module_name, internal_name>, <func_name, parameters> ]
        :param module:
        :param internal_name:
        :param func_name:
        :param args:
        '''
        self.funcs[','.join([str(module), internal_name])] = [func_name, args]

    def get_var(self, name):
        '''
        Gets value of a variable
        :param name:
        '''
        self.check_var(name)
        return self.vars[name]
    
    def check_func(self, module, internal_name):
        key = ','.join([module, internal_name])
        if not key in self.funcs:
            raise "Function {0} does not exist".format(key)
        return True
    
    def get_func(self, module, internal_name):
        '''
        Gets function by key (module + internal_name)
        :param module:
        :param name:
        '''
        self.check_func(module, internal_name)
        return self.funcs[','.join([module, internal_name])]
    
    def get_funcs(self, module_name):
        '''
        Gets all functions in a module.
        :param module_name: Name of the module
        '''
        return [{k:v} for k,v in self.funcs.items() if k.split(',')[0] == module_name]     
    
    def get_modbyinternalname(self, internal_name):
        '''
        Get a module name by internal_name
        :param internal_name:
        '''
        for k, v in self.funcs.items():
            mod_func = split(k)
            if mod_func[1] == funcname:
                return mod_func[0]
            
    def get_module_by_funcname(self, func_name):
        '''
        Get module name by the function name.
        :param func_name:
        '''
        for k, v in self.funcs.items():
            if v[0] == func_name:
                mod_func = k.split(',')
                return mod_func[0]
        raise "Function {0} does not exist.".format(func_name)
        
    def __str__(self):
        '''
        A string representation of this table.
        '''
        display = ""
        if self.vars:
            sym_name = "Symbol Name"
            sym_len = max(max(len(i) for i in self.vars), len(sym_name))
            
            sym_value = "Value"
            value_len = max(max(len(str(v)) for i,v in self.vars.items()), len(sym_value))
    
            # print table header for vars
            display = "{0:3s} | {1:^{2}s} | {3:^{4}s}".format(" No", sym_name, sym_len, sym_value, value_len)
            display += ("\n-------------------" + "-" * (sym_len + value_len))
            # print symbol table
            i = 1
            for k, v in self.vars.items():
                display += "\n{0:3d} | {1:^{2}s} | {3:^{4}s}".format(i, k, sym_len, str(v), value_len)
                i += 1

        if self.funcs:
            mod_name = "Module name"
            mod_len = max(max(len(i.split(',')[0]) for i in self.funcs), len(mod_name))
         
            internal_name = "Internal Name"
            internal_len = max(max(len(i.split(',')[1]) for i in self.funcs), len(internal_name))
            
            func_name = "Function Name"
            func_len = max(max(len(v[0]) for i,v in self.funcs.items()), len(func_name))
           
            param_names = "Parameters"
            param_len = len(param_names)
            l = 0
            for k, a in self.funcs.items():
                for v in a[1]:
                    l += len(v)
                param_len = max(param_len, l)
            
            # print table header for vars
            display += "\n\n{0:3s} | {1:^{2}s} | {3:^{4}s} | {5:^{6}s} | {7:^{8}s}".format(" No", mod_name, mod_len, internal_name, internal_len, func_name, func_len, param_names, param_len)
            display += ("\n-------------------" + "-" * (mod_len + internal_len + func_len + param_len))
            # print symbol table
            i = 1
            for k, v in self.funcs.items():
                modfunc = k.split(',')
                
                parameters = ""
                for p in v[1]:
                    if parameters == "":
                        parameters = "{0}".format(p)
                    else:
                        parameters += ", {0}".format(p)
                display += "\n{0:3d} | {1:^{2}s} | {3:^{4}s} | {5:^{6}s} | {7:^{8}s}".format(i, modfunc[0], mod_len, modfunc[1], internal_len, v[0], func_len, parameters, param_len)
                i += 1
                
        return display

        
class Context:
    '''
    The context for parsing and interpretation.
    '''
    def __init__(self):
        '''
        Initializes this object.
        :param parent:
        '''
        self.library = Library()
        self.reload()
    
    def reload(self):
        '''
        Reinitializes this object for new processing.
        '''
        self.symtab_stack = {}
        self.out = []
        self.err = []
        self.dci = []
        
    def get_var(self, name):
        '''
        Gets a variable from symbol stack and symbol table
        :param name:
        '''
        if threading.get_ident() in self.symtab_stack:
            for s in reversed(self.symtab_stack[threading.get_ident()]):
                if s.var_exists(name):
                    return s.get_var(name)
    
    def add_var(self, name, value):
        if not threading.get_ident() in self.symtab_stack:
            self.symtab_stack[threading.get_ident()] = [SymbolTable()]
        return self.symtab_stack[threading.get_ident()][-1].add_var(name, value)
            
    def update_var(self, name, value):
        if threading.get_ident() in self.symtab_stack:
            for s in reversed(self.symtab_stack[threading.get_ident()]):
                if s.var_exists(name):
                    return s.update_var(name, value)
                    
    def var_exists(self, name):
        '''
        Checks if a variable exists in any of the symbol tables.
        :param name: variable name
        '''
        if threading.get_ident() in self.symtab_stack:
            for s in reversed(self.symtab_stack[threading.get_ident()]):
                if s.var_exists(name):
                    return True
    
    def append_local_symtab(self):
        '''
        Appends a new symbol table to the symbol table stack.
        '''
        if threading.get_ident() in self.symtab_stack:
            self.symtab_stack[threading.get_ident()].append(SymbolTable())
        else:
            self.symtab_stack[threading.get_ident()] = [SymbolTable()]
        return self.symtab_stack[threading.get_ident()][len(self.symtab_stack[threading.get_ident()]) - 1]
    
    def pop_local_symtab(self):
        '''
        Pop a symbol table from the symbol table stack.
        '''
        if threading.get_ident() in self.symtab_stack:
            if self.symtab_stack[threading.get_ident()]:
                self.symtab_stack[threading.get_ident()].pop() 
        
    def load_library(self, library_def_dir_or_file):
        self.library = Library.load(library_def_dir_or_file)
                   
    def iequal(self, str1, str2):
        '''
        Compares two strings for case insensitive equality.
        :param str1:
        :param str2:
        '''
        if str1 == None:
            return str2 == None
        if str2 == None:
            return str1 == None
        return str1.lower() == str2.lower()
    
    def write(self, *args):
        '''
        Writes a line of strings in out context.
        '''
        self.out.append("{0}".format(', '.join(map(str, args))))
    
    def error(self, *args):
        '''
        Writes a line of strings in err context.
        '''
        self.err.append("{0}".format(', '.join(map(str, args))))

    def append_dci(self, server, user, password):
        self.dci.append([server, user, password])
    
    def pop_dci(self):
        if self.dci:
            return self.dci.pop()
    
    def get_activedci(self):
        if self.dci:
            return None
        return self.dci[-1]
            
class PhenoWLCodeGenerator:
    '''
    The code generator for PhenoWL DSL
    '''
    def __init__(self):
        self.context = Context()
        self.code = ''
        self.imports = set()
        self.indent = 0

    def get_params(self, expr):
        v = []
        for e in expr:
            v.append(self.eval(e))
        return v
            
    def indent_stmt(self, str):
        return " " * self.indent + str
    
    def dofunc(self, expr):
        '''
        Execute func expression.
        :param expr:
        '''
        function = expr[0] if len(expr) < 3 else expr[1]
        package = expr[0][:-1] if len(expr) > 2 else None
        
        params = expr[1] if len(expr) < 3 else expr[2]
        v = self.get_params(params)
        
        # call task if exists
        if package is None and function in self.context.library.tasks:
            return self.context.library.code_run_task(function, v, self.dotaskstmt)

        if not self.context.library.check_function(function, package):
            raise Exception(r"'{0}' doesn't exist.".format(function))
            
        return self.context.library.code_func(self.context, package, function, v)
    
    def dorelexpr(self, expr):
        '''
        Executes relative expression.
        :param expr:
        '''
        left = self.eval(expr[0])
        right = self.eval(expr[2])
        operator = expr[1]
        if operator == '<':
            return "{0} < {1}".format(str(left), str(right))
        elif operator == '>':
            return "{0} > {1}".format(str(left), str(right))
        elif operator == '<=':
            return "{0} <= {1}".format(str(left), str(right))
        elif operator == '>=':
            return "{0} >= {1}".format(str(left), str(right))
        else:
            return "{0} == {1}".format(str(left), str(right))
    
    def doand(self, expr):
        '''
        Executes "and" expression.
        :param expr:
        '''
        if expr is empty:
            return True
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['ANDEXPR'] + left
        left = self.eval(left)
        return "{0} and {1}".format(str(left), str(right))
    
    def dopar(self, expr):
        code = 'taskManager = TaskManager()\n'
#         for stmt in expr:
#             code += 'taskManager.submit_func(lambda: ' + self.eval(stmt) + ')\n'
        return code
    
    def dopar_stmt(self, expr):
        '''
        Execute a parallel expression.
        :param expr:
        '''
        self.run_multstmt(lambda: self.eval(expr))
    
    def run_multstmt(self, f):
        return f()

    def dolog(self, expr):
        '''
        Executes a logical expression.
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['LOGEXPR'] + left
        left = self.eval(left)
        return "{0} or {1}".format(str(left), str(right))
    
    def domult(self, expr):
        '''
        Executes a multiplication/division operation
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['MULTEXPR'] + left
        left = self.eval(left)
        return "{0} / {1}".format(str(left), str(right)) if expr[-2] == '/' else "{0} * {1}".format(str(left), str(right))

    def doarithmetic(self, expr):
        '''
        Executes arithmetic operation.
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['NUMEXPR'] + left
        left = self.eval(left)
        return "{0} + {1}".format(str(left), str(right)) if expr[-2] == '+' else "{0} - {1}".format(str(left), str(right))
    
    def doif(self, expr):
        '''
        Executes if statement.
        :param expr:
        '''
        code = "if " + self.eval(expr[0]) + ":\n"
        code += self.run_multstmt(lambda: self.eval(expr[1]))
        if len(expr) > 3:
            code += "else:\n"
            code += self.run_multstmt(lambda: self.eval(expr[3]))
        return code
    
    def dolock(self, expr):
        if not self.context.symtab.var_exists(expr[0]) or not isinstance(self.context.symtab.get_var(expr[0]), _thread.RLock):
            self.context.symtab.add_var(expr[0], threading.RLock())    
        with self.context.symtab.get_var(expr[0]):
            self.eval(expr[1])
        pass
        
    def doassign(self, expr):
        '''
        Evaluates an assignment expression.
        :param expr:
        '''
        return "{0} = {1}".format(expr[0], self.eval(expr[1]))
        
    def dofor(self, expr):
        '''
        Execute a for expression.
        :param expr:
        '''
        code = "for {0} in {1}:\n".format(self.eval(expr[0]), self.eval(expr[1]))
        code += self.run_multstmt(lambda: self.eval(expr[2]))
        return code
    
    def eval_value(self, str_value):
        '''
        Evaluate a single expression for value.
        :param str_value:
        '''
        return str_value
    
    def dolist(self, expr):
        '''
        Executes a list operation.
        :param expr:
        '''
        v = []
        for e in expr:
            v.append(self.eval(e))
        return v
    
    def dolistidx(self, expr):
        val = self.context.get_var(expr[0])
        return val[self.eval(expr[1])]
    
    def dostmt(self, expr):
        if len(expr) > 1:
            logging.debug("Processing line: {0}".format(expr[0]))
            self.line = int(expr[0])
            return self.indent_stmt(self.eval(expr[1:])) + '\n'

    def dotaskdefstmt(self, expr):
        if not expr[0]:
            v = self.get_params(expr[1])
            return self.dotaskstmt(expr, v)
        else:
            self.context.library.add_task(expr[0], expr)
            return ''
    
    def dotaskstmt(self, expr, args):
        server = args[0] if len(args) > 0 else None
        user = args[1] if len(args) > 1 else None
        password = args[2] if len(args) > 2 else None
        
        if not server:
            server = self.eval(expr[1][0]) if len(expr[1]) > 0 else None
        if not user:
            user = self.eval(expr[1][1]) if len(expr[1]) > 1 else None
        if not password:
            password = self.eval(expr[1][2]) if len(expr[1]) > 2 else None
        
        try:
            self.context.append_dci(server, user, password)
            return 'if True:\n' + self.eval(expr[2])
        finally:
            self.context.pop_dci()
                    
    def eval(self, expr):        
        '''
        Evaluate an expression
        :param expr: The expression in AST tree form.
        '''
        if not isinstance(expr, list):
            return self.eval_value(expr)
        if not expr:
            return
        if len(expr) == 1:
            return self.eval(expr[0])
        if expr[0] == "FOR":
            return self.dofor(expr[1])
        elif expr[0] == "ASSIGN":
            return self.doassign(expr[1:])
        elif expr[0] == "CONST":
            return self.eval_value(expr[1])
        elif expr[0] == "NUMEXPR":
            return self.doarithmetic(expr[1:])
        elif expr[0] == "MULTEXPR":
            return self.domult(expr[1:])
        elif expr[0] == "CONCAT":
            return self.doarithmetic(expr[1:])
        elif expr[0] == "LOGEXPR":
            return self.dolog(expr[1:])
        elif expr[0] == "ANDEXPR":
            return self.doand(expr[1:])
        elif expr[0] == "RELEXPR":
            return self.dorelexpr(expr[1:])
        elif expr[0] == "IF":
            return self.doif(expr[1])
        elif expr[0] == "LIST":
            return self.dolist(expr[1])
        elif expr[0] == "FUNCCALL":
            code, imports = self.dofunc(expr[1])
            self.imports.update(imports)
            return code
        elif expr[0] == "LISTIDX":
            return self.dolistidx(expr[1])
        elif expr[0] == "PAR":
            return self.dopar(expr[1])
        elif expr[0] == "LOCK":
            return self.dolock(expr[1:])
        elif expr[0] == "STMT":
            return self.dostmt(expr[1:])
        elif expr[0] == "TASK":
            return self.dotaskdefstmt(expr[1:])
        elif expr[0] == "MULTISTMT":
            try:
                self.indent = int(expr[1].pop()) - 1
                return self.eval(expr[2:])
            finally:
                self.indent = int(expr[1].pop()) - 1
        else:
            code = ''
            for subexpr in expr:
                code += self.eval(subexpr)
            return code

    # Run it
    def run(self, prog):
        '''
        Run a new program.
        :param prog: Pyparsing ParseResults
        '''
        try:
            self.context.reload()
            stmt = prog.asList()
            code = self.eval(stmt)
            imports = ''
            for i in self.imports:
                imports = i + '\n';
            self.context.out = imports + '\n' + code 
        except Exception as err:
            self.context.err.append("Error at line {0}: {1}".format(self.line, err))

class PhenoWLInterpreter:
    '''
    The interpreter for PhenoWL DSL
    '''
    def __init__(self):
        self.context = Context()
        self.line = 0
    
    def get_params(self, expr):
        v = []
        for e in expr:
            v.append(self.eval(e))
        return v
        
    def dofunc(self, expr):
        '''
        Execute func expression.
        :param expr:
        '''
        function = expr[0] if len(expr) < 3 else expr[1]
        package = expr[0][:-1] if len(expr) > 2 else None
        
        params = expr[1] if len(expr) < 3 else expr[2]
        v = self.get_params(params)
        
        # call task if exists
        if package is None and function in self.context.library.tasks:
            return self.context.library.run_task(function, v, self.dotaskstmt)

        if not self.context.library.check_function(function, package):
            raise Exception(r"'{0}' doesn't exist.".format(function))
            
        return self.context.library.call_func(self.context, package, function, v)

    def dorelexpr(self, expr):
        '''
        Executes relative expression.
        :param expr:
        '''
        left = self.eval(expr[0])
        right = self.eval(expr[2])
        operator = expr[1]
        if operator == '<':
            return left < right
        elif operator == '>':
            return left > right
        elif operator == '<=':
            return left <= right
        elif operator == '>=':
            return left >= right
        else:
            return left == right
    
    def doand(self, expr):
        '''
        Executes "and" expression.
        :param expr:
        '''
        if expr is empty:
            return True
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['ANDEXPR'] + left
        left = self.eval(left)
        return left and right
    
    def dopar(self, expr):
        taskManager = TaskManager() 
        for stmt in expr:
            taskManager.submit_func(self.dopar_stmt, stmt)
        taskManager.wait();
            
    def dopar_stmt(self, expr):
        '''
        Execute a for expression.
        :param expr:
        '''
        self.run_multstmt(lambda: self.eval(expr))
    
    def run_multstmt(self, f):
        local_symtab = self.context.append_local_symtab()
        try:
            f()
        finally:
            self.context.pop_local_symtab()
            
    def dolog(self, expr):
        '''
        Executes a logical expression.
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['LOGEXPR'] + left
        left = self.eval(left)
        return left or right
    
    def domult(self, expr):
        '''
        Executes a multiplication/division operation
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['MULTEXPR'] + left
        left = self.eval(left)
        return left / right if expr[-2] == '/' else left * right

    def doarithmetic(self, expr):
        '''
        Executes arithmetic operation.
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['NUMEXPR'] + left
        left = self.eval(left)
        return left + right if expr[-2] == '+' else left - right
    
    def doif(self, expr):
        '''
        Executes if statement.
        :param expr:
        '''
        cond = self.eval(expr[0])
        if cond:
            self.run_multstmt(lambda: self.eval(expr[1]))
        elif len(expr) > 3 and expr[3]:
            self.run_multstmt(lambda: self.eval(expr[3]))
    
    def dolock(self, expr):
        if not self.context.symtab.var_exists(expr[0]) or not isinstance(self.context.symtab.get_var(expr[0]), _thread.RLock):
            self.context.symtab.add_var(expr[0], threading.RLock())    
        with self.context.symtab.get_var(expr[0]):
            self.eval(expr[1])
        pass
        
    def doassign(self, left, right):
        '''
        Evaluates an assignment expression.
        :param expr:
        '''
        if len(left) == 1:
            self.context.add_var(left[0], self.eval(right))
        elif left[0] == 'LISTIDX':
            left = left[1]
            idx = self.eval(left[1])
            if self.context.var_exists(left[0]):
                v = self.context.get_var(left[0])
                if isinstance(v, list):
                    while len(v) <= idx:
                        v.append(None)
                    v[int(idx)] = self.eval(right)
                elif isinstance(v, dict):
                    v[idx] = self.eval(right)
                else:
                    raise "Not a list or dictionary"
            else:
                v = []
                while len(v) <= idx:
                    v.append(None)
                v[int(idx)] = self.eval(right)
                self.context.add_var(left[0], v)
        
        
    def dofor(self, expr):
        '''
        Execute a for expression.
        :param expr:
        '''
        local_symtab = self.context.append_local_symtab()
        local_symtab.add_var(expr[0], None)
        try:
            for var in self.eval(expr[1]):
                local_symtab.update_var(expr[0], var)
                self.eval(expr[2])
        finally:
            self.context.pop_local_symtab()
    
    def eval_value(self, str_value):
        '''
        Evaluate a single expression for value.
        :param str_value:
        '''
        try:
            t = ast.literal_eval(str_value)
            if type(t) in [int, float, bool, complex]:
                if t in set((True, False)):
                    bool(str_value)
                if type(t) is int:
                    return int(str_value)
                if type(t) is float:
                    return float(t)
                if type(t) is complex:
                    return complex(t)
            else:
                if len(str_value) > 1:
                    if (str_value.startswith("'") and str_value.endswith("'")) or (str_value.startswith('"') and str_value.endswith('"')):
                        return str_value[1:-1]
            return str_value
        except ValueError:
            if self.context.var_exists(str_value):
                return self.context.get_var(str_value)
            return str_value
    
    def dolist(self, expr):
        '''
        Executes a list operation.
        :param expr:
        '''
        v = []
        for e in expr:
            v.append(self.eval(e))
        return v
    
    def remove_single_item_list(self, expr):
        if not isinstance(expr, list):
            return expr
        if len(expr) == 1:
            return self.remove_single_item_list(expr[0])
        return expr
        
    def dodict(self, expr):
        '''
        Executes a list operation.
        :param expr:
        '''
        v = {}
        for e in expr:
            #e = self.remove_single_item_list(e)
            v[self.eval(e[0])] = self.eval(e[1])
        return v
    
    def dolistidx(self, expr):
        val = self.context.get_var(expr[0])
        return val[self.eval(expr[1])]
    
    def dostmt(self, expr):
        if len(expr) > 1:
            logging.debug("Processing line: {0}".format(expr[0]))
            self.line = int(expr[0])
            return self.eval(expr[1:])
    
    #===========================================================================
    # dotaskdefstmt
    # if task has no name, it will be called at once.
    # if task has a name, it will be called like a function call afterwards
    #===========================================================================
    def dotaskdefstmt(self, expr):
        if not expr[0]:
            v = self.get_params(expr[1])
            return self.dotaskstmt(expr, v)
        else:
            self.context.library.add_task(expr[0], expr)
    
    def dotaskstmt(self, expr, args):
        server = args[0] if len(args) > 0 else None
        user = args[1] if len(args) > 1 else None
        password = args[2] if len(args) > 2 else None
        
        if not server:
            server = self.eval(expr[1][0]) if len(expr[1]) > 0 else None
        if not user:
            user = self.eval(expr[1][1]) if len(expr[1]) > 1 else None
        if not password:
            password = self.eval(expr[1][2]) if len(expr[1]) > 2 else None
        
        self.context.append_dci(server, user, password)
        try:
            return self.eval(expr[2])
        finally:
            self.context.pop_dci()
            
    def eval(self, expr):        
        '''
        Evaluate an expression
        :param expr: The expression in AST tree form.
        '''
        if not isinstance(expr, list):
            return self.eval_value(expr)
        if not expr:
            return
        if len(expr) == 1:
            if expr[0] == "LISTEXPR":
                return list()
            elif expr[0] == "DICTEXPR":
                return dict()
            else:
                return self.eval(expr[0])
        if expr[0] == "FOR":
            return self.dofor(expr[1])
        elif expr[0] == "ASSIGN":
            return self.doassign(expr[1], expr[2])
        elif expr[0] == "CONST":
            return self.eval_value(expr[1])
        elif expr[0] == "NUMEXPR":
            return self.doarithmetic(expr[1:])
        elif expr[0] == "MULTEXPR":
            return self.domult(expr[1:])
        elif expr[0] == "CONCAT":
            return self.doarithmetic(expr[1:])
        elif expr[0] == "LOGEXPR":
            return self.dolog(expr[1:])
        elif expr[0] == "ANDEXPR":
            return self.doand(expr[1:])
        elif expr[0] == "RELEXPR":
            return self.dorelexpr(expr[1:])
        elif expr[0] == "IF":
            return self.doif(expr[1])
        elif expr[0] == "LISTEXPR":
            return self.dolist(expr[1:])
        elif expr[0] == "DICTEXPR":
            return self.dodict(expr[1:])
        elif expr[0] == "FUNCCALL":
            return self.dofunc(expr[1])
        elif expr[0] == "LISTIDX":
            return self.dolistidx(expr[1])
        elif expr[0] == "PAR":
            return self.dopar(expr[1])
        elif expr[0] == "LOCK":
            return self.dolock(expr[1:])
        elif expr[0] == "STMT":
            return self.dostmt(expr[1:])
        elif expr[0] == "MULTISTMT":
            return self.eval(expr[2:])
        elif expr[0] == "TASK":
            return self.dotaskdefstmt(expr[1:])
        else:
            val = []
            for subexpr in expr:
                val.append(self.eval(subexpr))
            return val

    # Run it
    def run(self, prog):
        '''
        Run a new program.
        :param prog: Pyparsing ParseResults
        '''
        try:
            self.context.reload()
            stmt = prog.asList()
            self.eval(stmt)
        except Exception as err:
            self.context.err.append("Error at line {0}: {1}".format(self.line, err))

class BasicGrammar():
    '''
    The base grammar for PhenoWL parser.
    '''
    RELATIONAL_OPERATORS = "< > <= >= == !=".split()
    def __init__(self):
        self.build_grammar()
    
    def build_grammar(self):
        
        self.identifier = Word(alphas + "_", alphanums + "_")
        
        point = Literal('.')
        e = CaselessLiteral('E')
        plusorminus = Literal('+') | Literal('-')

        self.number = Word(nums)
        self.integer = Combine(Optional(plusorminus) + self.number).setParseAction(lambda x : x[0])
        self.floatnumber = Combine(self.integer + Optional(point + Optional(self.number)) + Optional(e + self.integer)).setParseAction(lambda x : x[0])
        self.string = quotedString.setParseAction(lambda x : x[0])
        self.constant = Group((self.floatnumber | self.integer | self.string).setParseAction(lambda t : ['CONST'] + [t[0]]))
        self.relop = oneOf(BasicGrammar.RELATIONAL_OPERATORS)
        self.multop = oneOf("* /")
        self.addop = oneOf("+ -")

        # Definitions of rules for numeric expressions
        self.expr = Forward()
        self.multexpr = Forward()
        self.numexpr = Forward()
        self.arguments = Forward()
        self.stringaddexpr = Forward()
        modpref = Combine(OneOrMore(self.identifier + Literal(".")))
        self.funccall = Group((Optional(modpref) + self.identifier + FollowedBy("(")) + 
                              Group(Suppress("(") + Optional(self.arguments) + Suppress(")"))).setParseAction(lambda t : ['FUNCCALL'] + t.asList())
        self.listidx = Group(self.identifier + Suppress("[") + self.expr + Suppress("]")).setParseAction(lambda t : ['LISTIDX'] + t.asList())
        self.dictdecl = Forward()
        self.listdecl = Forward() 
        
        pi = CaselessKeyword( "PI" )
        fnumber = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?")
        plus, minus, mult, div = map(Literal, "+-*/")
        self.lpar, self.rpar = map(Suppress, "()")
        expop = Literal( "**" )
        
        parexpr = self.lpar + self.expr + self.rpar
        atom = (( pi | e | fnumber | self.string | self.identifier + parexpr | self.identifier) | Group(parexpr))
        
        # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...", we get right-to-left exponents, instead of left-to-righ
        # that is, 2^3^2 = 2^(3^2), not (2^3)^2.
        factor = Forward()
        factor << atom + ZeroOrMore(expop + factor )
        
        self.multexpr << Group((factor + ZeroOrMore(self.multop + factor)).setParseAction(lambda t: ['MULTEXPR'] + t.asList()))
        self.numexpr << Group((self.multexpr + ZeroOrMore(self.addop + self.multexpr)).setParseAction(lambda t: ['NUMEXPR'] + t.asList()))
        self.stringaddexpr << Group((self.string + ZeroOrMore(Literal("+") + (self.identifier | self.string))).setParseAction(lambda t: ['CONCAT'] + t.asList()))
                
        self.expr << (self.stringaddexpr | self.string | self.funccall | self.listidx | self.listdecl | self.dictdecl | self.numexpr).setParseAction(lambda x : x.asList())
        
        self.namedarg = Group(self.identifier + Literal("=") + self.expr).setParseAction(lambda t: ['NAMEDARG'] + t.asList())
        self.arguments << delimitedList(Group(self.namedarg | self.expr))
        
        # Definitions of rules for logical expressions (these are without parenthesis support)
        self.andexpr = Forward()
        self.logexpr = Forward()
        self.relexpr = Group((Suppress(Optional(self.lpar)) + self.numexpr + self.relop + self.numexpr + Suppress(Optional(self.rpar))).setParseAction(lambda t: ['RELEXPR'] + t.asList()))
        self.andexpr << Group((self.relexpr("exp") + ZeroOrMore(Keyword("and") + self.relexpr("exp"))).setParseAction(lambda t : ["ANDEXPR"] + t.asList()))
        self.logexpr << Group((self.andexpr("exp") + ZeroOrMore(Keyword("or") + self.andexpr("exp"))).setParseAction(lambda t : ["LOGEXPR"] + t.asList())) 
        #Group(self.andexpr("exp") + ZeroOrMore(Keyword("or") + self.andexpr("exp"))).setParseAction(lambda t : ["LOGEXPR"] + t.asList())

        # Definitions of rules for statements
        self.stmt = Forward()
        self.stmtlist = Forward()
        self.retstmt = (Keyword("return") + self.expr("exp"))
#       
        self.listdecl << (Suppress("[") + Optional(delimitedList(Group(self.expr))) + Suppress("]")).setParseAction(lambda t: ["LISTEXPR"] + t.asList())
        self.dictdecl << (Suppress("{") + Optional(delimitedList(Group(self.expr + Suppress(Literal(":")) + Group(self.dictdecl | self.expr)))) + Suppress("}")).setParseAction(lambda t: ["DICTEXPR"] + t.asList())

        self.assignstmt = (Group(self.listidx | self.identifier) + Suppress(Literal("=")) + Group(self.expr | self.listidx | self.listdecl | self.dictdecl)).setParseAction(lambda t: ['ASSIGN'] + t.asList())
        
        self.funccallstmt = self.funccall
        
    def build_program(self):
        self.stmt << Group((self.taskdefstmt | self.parstmt | self.retstmt | self.ifstmt | self.forstmt | self.lockstmt | self.funccallstmt | self.assignstmt | self.expr).setParseAction(lambda s,l,t :  ['STMT'] + [lineno(l, s)] + [t]))
        self.stmtlist << ZeroOrMore(self.stmt)
        self.program = self.stmtlist

class PythonGrammar(BasicGrammar):
    '''
    A Python style grammar.
    '''
    
    def __init__(self):
        self.build_grammar()
    
    def parseCompoundStmt(self, s, l, t):
        expr = ["MULTISTMT"] + [list(self.indentStack)] + t.asList()
        self.indentStack.pop()
        return expr
    
    def build_grammar(self):
        super().build_grammar()
        
        self.indentStack = [1]
        self.compoundstmt = Group(myIndentedBlock(self.stmt, self.indentStack, True).setParseAction(self.parseCompoundStmt))
        self.ifstmt = Group(Suppress("if") + self.logexpr  + Suppress(":") + self.compoundstmt + Optional((Suppress("else") + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['ELSE'] + t.asList()))).setParseAction(lambda t : ['IF'] + t.asList())
        self.forstmt = Group(Suppress("for") + self.identifier("var") + Suppress("in") + Group(self.expr("range"))  + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['FOR'] + t.asList())
        self.parstmt = Group(Suppress("parallel") + Suppress(":") + self.compoundstmt + ZeroOrMore(Suppress("with:") + self.compoundstmt)).setParseAction(lambda t : ['PAR'] + t.asList())
        self.lockstmt = (Suppress("lock") + Suppress(self.lpar) + self.identifier + Suppress(self.rpar) + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['LOCK'] + t.asList())
        self.taskdefstmt = (Suppress("task") + Optional(self.identifier, None) + Suppress("(")  + Group(Optional(self.arguments)) + Suppress(")") + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['TASK'] + t.asList())         
        super().build_program()                                 
                                 
class PhenoWLGrammar(BasicGrammar):
    '''
    The PhenoWL grammar.
    '''
    
    def __init__(self):
        self.build_grammar()
    
    def build_grammar(self):
        super().build_grammar()
        
        self.compoundstmt = Group(Suppress("{") + self.stmtlist + Suppress("}"))
        self.ifstmt = Group(Keyword("if") + self.logexpr + self.compoundstmt + Group(Optional(Keyword("else") + self.compoundstmt)).setParseAction(lambda t : ['ELSE'] + t.asList())).setParseAction(lambda t : ['IF'] + t.asList())
        self.forstmt = Group(Keyword("for") + self.identifier("var") + Keyword("in") + Group(self.expr("range")) + self.compoundstmt).setParseAction(lambda t : ['FOR'] + t.asList())                                 
        self.parstmt = Group(Keyword("parallel") + self.compoundstmt).setParseAction(lambda t : ['PAR'] + t.asList())
                                 
        super().build_program()
                 
class PhenoWLParser(object):
    '''
    The parser for PhenoWL DSL.
    '''

    def __init__(self, grammar = None):
        if grammar is None:
            self.grammar = PhenoWLGrammar()
        else:
            self.grammar = grammar
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
