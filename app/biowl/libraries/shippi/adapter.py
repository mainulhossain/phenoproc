from . import runner

def prepare_args(function, *arguments):    
    if function == 'registerimage':
    #                     p = IOHelper.getServerPath(arguments[0])
    #                     server = p[0] if p[0] else '127.0.0.1'
    #                     src = p[1]
    #                     dest = arguments[1]
    #                     uname = arguments[2]
    #                     password = arguments[3]
        if len(arguments) < 5:
            raise ValueError("Wrong number of arguments for image registration.")
        
        arguments = list(arguments)
        arguments.insert(0, function)
        if len(arguments) < 7:
            arguments.append("*")
        if len(arguments) < 8:
            arguments.append(4)
        if len(arguments) < 9:
            arguments.append(0.75)
        if len(arguments) < 10:
            arguments.append(0)
        if len(arguments) < 11:
            arguments.append(0)
        return arguments
    
def execute_runner(function, *arguments):
    dci = None
    if context.dci:
        dci = context.get_activedci()
    if dci and dci[0]:
        return runner.run_shippi(*arguments)
    else:
        return runner.run_hippi(*arguments)

def register_image(function, *arguments):
    return execute_runner(function, arguments)