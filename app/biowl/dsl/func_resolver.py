from importlib import import_module
from itertools import chain
import json
from os import path, getcwd
import os
import subprocess

from ..fileop import IOHelper
from ..exechelper import func_exec_run

#from phenoparser import Context

def load_module(modulename):
    '''
    Load a module dynamically from a string module name.
    It was first implemented with __import__, but later
    replaced by importlib.import_module.
    :param modulename:
    '''
    #if modulename not in sys.modules:
    #name = "package." + modulename
    #return __import__(modulename, fromlist=[''])
    return import_module(modulename)

class Function():
    def __init__(self, name, internal, package = None, module = None, params = [], example = None, desc = None, runmode = None):
        self.name = name
        self.internal = internal
        self.package = package
        self.module = module
        self.params = params
        self.example = example
        self.desc = desc
        self.runmode = runmode
        
class Library():
    def __init__(self, funcs = {}):
        self.funcs = funcs
        self.tasks = {}
        self.localdir = path.join(path.abspath(path.dirname(__file__)), 'storage')
    
    def add_task(self, name, expr):
        self.tasks[name] = expr
    
    def run_task(self, name, args, dotaskstmt):
        if name in self.tasks:
            return dotaskstmt(self.tasks[name][1:], args)

    def code_run_task(self, name, args, dotaskstmt):
        if name in self.tasks:
            return dotaskstmt(self.tasks[name], args), set()
            
    @staticmethod
    def load(library_def_file):
        library = Library()
        library.funcs = Library.load_funcs_recursive(library_def_file)
        return library
    
    @staticmethod
    def load_funcs_recursive(library_def_file):
        if os.path.isfile(library_def_file):
            return Library.load_funcs(library_def_file)
        
        all_funcs = {}
        for f in os.listdir(library_def_file):
            funcs = Library.load_funcs_recursive(os.path.join(library_def_file, f))
            for k,v in funcs.items():
                if k in all_funcs:
                    all_funcs[k].extend(v)
                else:
                    all_funcs[k] = v if isinstance(v, list) else [v]
        return all_funcs
       
    @staticmethod
    def load_funcs(library_def_file):
        funcs = {}
        try:
            if not os.path.isfile(library_def_file) or not library_def_file.endswith(".json"):
                return funcs
            
            with open(library_def_file, 'r') as json_data:
                d = json.load(json_data)
                libraries = d["functions"]
                libraries = sorted(libraries, key = lambda k : k['package'].lower())
                for f in libraries:
                    name = f["name"] if f.get("name") else f["internal"]
                    internal = f["internal"] if f.get("internal") else f["name"]
                    module = f["module"] if f.get("module") else None
                    package = f["package"] if f.get("package") else None
                    example = f["example"] if f.get("example") else None
                    desc = f["desc"] if f.get("desc") else None
                    runmode = f["runmode"] if f.get("runmode") else None
                    params = []
                    if f.get("params"):
                        for param in f["params"]:
                            params.append(param)
                    func = Function(name, internal, package, module, params, example, desc, runmode)
                    if name.lower() in funcs:
                        funcs[name.lower()].extend([func])
                    else:
                        funcs[name.lower()] = [func]
        finally:
            return funcs
    
    def func_to_internal_name(self, funcname):
        for f in self.funcs:
            if f.get("name") and self.iequal(f["name"], funcname):
                return f["internal"]
            
    def get_function(self, name, package = None):
        if package is not None:
            for func in self.funcs[name.lower()]:
                if func.package == package:
                    return [func]
        else:
            return self.funcs[name.lower()]
    
    def check_function(self, name, package = None):
        functions = self.get_function(name, package)
        return functions is not None and len(functions) > 0
        
    def funcs_flat(self):
        funcs = []
        for v in self.funcs.values():
            funcs.extend(v)
        return funcs
        
    @staticmethod
    def split_args(arguments):
        args = []
        kwargs = {}
        for arg in arguments:
            if isinstance(arg, tuple):
                kwargs[arg[0]] = arg[1]
            else:
                args.append(arg)
        return args, kwargs
       
    def call_func(self, context, package, function, args):
        '''
        Call a function from a module.
        :param context: The context for output and error
        :param package: The name of the package. If it's empty, local function is called    
        :param function: Name of the function
        :param args: The arguments for the function
        '''
        arguments, kwargs = Library.split_args(args)
        
        if not package or package == "None":
            if function.lower() == "print":
                return context.write(*arguments)
            elif function.lower() == "range":
                return range(*arguments)
            elif function.lower() == "read":
                if not arguments:
                    raise "Read must have one argument."
                return IOHelper.read(arguments[0])
            elif function.lower() == "write":
                if len(arguments) < 2:
                    raise "Write must have two arguments."
                return IOHelper.write(arguments[0], arguments[1])
            elif function.lower() == "getfiles":
                return IOHelper.get_files(arguments[0])
            elif function.lower() == "getfolders":
                return IOHelper.get_folders(arguments[0])
            elif function.lower() == "createfolder":
                return IOHelper.create_folder(arguments[0])
            elif function.lower() == "remove":
                return IOHelper.remove(arguments[0])
            elif function.lower() == "makedirs":
                return IOHelper.makedirs(arguments[0])
            elif function.lower() == "getcwd":
                return getcwd()
            elif function.lower() == "len":
                return len(arguments[0])
            elif function.lower() == "exec":
                return func_exec_run(arguments[0], *arguments[1:])
            #    return func_exec(arguments[0], *arguments[1:])
#             else:
#                 raise "{0} function not implemented".format(function)
    #             possibles = globals().copy()
    #             possibles.update(locals())
    #             function = possibles.get(function)
    #             return function(*arguments)
        func = self.get_function(function, package)
        module_obj = load_module(func[0].module)
        function = getattr(module_obj, func[0].internal)
        if context.dci and context.dci[-1] and func[0].runmode == 'dist':
            arguments = context.dci[-1] + arguments
        return function(*arguments)

    def code_func(self, context, package, function, arguments):
        '''
        Call a function from a module.
        :param context: The context for output and error
        :param package: The name of the package. If it's empty, local function is called    
        :param function: Name of the function
        :param arguments: The arguments for the function
        '''
        imports = set()
        args = ','.join(arguments)
        code = ''
        if not package or package == "None":
            if function.lower() == "print":
                code = "print({0})".format(args)
            elif function.lower() == "range":
                code = "range({0})".format(args)
            elif function.lower() == "read":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.read({0})".format(args)
            elif function.lower() == "write":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.write({0})".format(args)
            elif function.lower() == "getfiles":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.getfiles({0})".format(args)
            elif function.lower() == "getfolders":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.getfolders({0})".format(args)
            elif function.lower() == "remove":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.remove({0})".format(args)
            elif function.lower() == "createfolder":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.makedirs({0})".format(args)
            elif function.lower() == "getcwd":
                imports.add("import os")
                code = "os.getcwd()"
            elif function.lower() == "len":
                code = "len({0})".format(arguments[0])
            elif function.lower() == "exec":
                imports.add("import subprocess")
                code =  "func_exec_run({0}, {1})".format(arguments[0], arguments[1])

        if code:
            return code, imports
        
        imports.add("from importlib import import_module")
        func = self.get_function(function, package)
        code = "module_obj = load_module({0})\n".format(func[0].module)
        code += "function = getattr(module_obj, {0})\n".format(func[0].internal)
        if context.dci and context.dci[-1] and func.runmode == 'distibuted':
            args = [context.dci[-1]] + args
        code += "function({0})".format(args)
        return code, imports
            
    def __repr__(self):
        return "Library: " + repr(self.funcs)
    def __getitem__(self, key):
        return self.funcs[key]
    def __setitem__(self, key, val):
        self.funcs[key] = val
    def __delitem__(self, key):
        del self.funcs[key]
    def __contains__(self, key):
        return key in self.funcs
    def __iter__(self):
        return iter(self.funcs.keys())

    def __str__(self):
        funcs = self.funcs_flat();
        #funcs = [num for elem in funcs for num in elem]
            
        if len(funcs) > 0:
            mod_name = "Module name"
            mod_len = max(max(len(i.module) if i.module is not None else 0 for i in funcs), len(mod_name))
         
            internal_name = "Internal Name"
            internal_len = max(max(len(i.internal) for i in funcs), len(internal_name))
            
            func_name = "Function Name"
            func_len = max(max(len(i.name) for i in funcs), len(func_name))
           
            param_names = "Parameters"
            param_len = len(param_names)
            l = 0
            for a in funcs:
                for v in a.params:
                    l += len(v)
                param_len = max(param_len, l)
            
            # print table header for vars
            display = "\n\n{0:3s} | {1:^{2}s} | {3:^{4}s} | {5:^{6}s} | {7:^{8}s}".format(" No", mod_name, mod_len, internal_name, internal_len, func_name, func_len, param_names, param_len)
            display += ("\n-------------------" + "-" * (mod_len + internal_len + func_len + param_len))
            # print symbol table
            
            i = 1
            for v in funcs:
                module = v.module if v.module is not None else "None"
                parameters = ""
                for p in v.params:
                    if parameters == "":
                        parameters = "{0}".format(p)
                    else:
                        parameters += ", {0}".format(p)
                display += "\n{0:3d} | {1:^{2}s} | {3:^{4}s} | {5:^{6}s} | {7:^{8}s}".format(i, module, mod_len, v.internal, internal_len, v.name, func_len, parameters, param_len)
                
                i += 1
                
        return display

