#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import ast
import builtins
import dis
import inspect
import pickle
import re
import sys
import textwrap
from collections import defaultdict
from logging import getLogger
from types import BuiltinFunctionType, CodeType, FunctionType, ModuleType
from typing import Any, Dict, Iterable, List, Set, Tuple, Union

import opcode

logger = getLogger(__name__)

STORE_GLOBAL = opcode.opmap["STORE_GLOBAL"]
DELETE_GLOBAL = opcode.opmap["DELETE_GLOBAL"]
LOAD_GLOBAL = opcode.opmap["LOAD_GLOBAL"]
GLOBAL_OPS = (STORE_GLOBAL, DELETE_GLOBAL, LOAD_GLOBAL)

CODE_AS_COMMENT_HINT = (
    "The following comment contains the UDF source code"
    " generated by snowpark-python for explanatory purposes.\n"
)
CODE_HEADER = """\
from __future__ import annotations
import pickle
"""


def get_func_references(func: FunctionType, ref_objects: Dict[str, Any]) -> None:
    """
    Get the objects references by target func, they could be methods, modules, classes, methods, global variables
    and its closures.

    Args:
        func: The target function to generate source code for.
        ref_objects: dict of objects referenced by the target function, key is the name and value is the object.
    """
    # 1. resolve function global references
    code_object = func.__code__
    globals_ref = extract_func_global_refs(
        func.__code__
    )  # get the names of the objects which func references
    globals = {
        k: func.__globals__[k]
        for k in globals_ref
        if k in func.__globals__  # retrieve the objects by names
        and k not in ref_objects
    }

    ref_objects.update(globals)

    # 2. resolve function closure references
    if func.__closure__ is not None:
        closures = {
            k: v
            for k, v in zip(
                code_object.co_freevars,
                list(map(lambda x: x.cell_contents, func.__closure__)),
            )
        }
        for k, v in closures.items():
            ref_objects[k] = v
            # if the closure item is a function and is not itself (recursive) and has not been visited
            if isinstance(v, FunctionType) and v != func:
                get_func_references(v, ref_objects)


def get_class_references(
    cls: type,
    func: FunctionType,
    ref_objects: Dict[str, Any],
    classes_to_generate: list,
    *,
    generate_code_for_class: bool = True,
) -> None:
    """
    To get the referenced objects of a class defined in the same module.
    A class could have methods, subclasses referencing other objects.

    Args:
        cls: The class to be analyzed to find references.
        func: The target function to generate source code for.
        ref_objects: dict of objects referenced by the target function, key is the name and value is the object.
        classes_to_generate: list of classes which are defined in the same module as the target function.
            Code generation is required for these classes.
        generate_code_for_class: Whether the source code for the class shall be generated.
    """
    func_module_name = func.__module__

    if generate_code_for_class:
        # order matters to classes_to_generate, when constructing source code,
        # referenced classes need to be defined first.
        classes_to_generate.insert(0, cls)

    inferred_classes: List[Tuple[Any, bool]] = []

    for base_class in cls.__bases__:
        base_class_name = base_class.__qualname__
        top_level_class = base_class_name.split(".")[0]
        if base_class_name == "object":
            continue
        elif base_class.__module__ == func_module_name:
            # if base class is from the same module, we need to parse the class as well as generate code for the class
            # False in the tuple means this is not a nested class
            inferred_classes.append((base_class, False))
        else:
            # if base class is from another module, we need to import it
            ref_objects[top_level_class] = func.__globals__[top_level_class]

    # __dict__ contains function, classmethod, classes attributes within a given class which
    # needs to be further analyzed
    for v in dict(cls.__dict__).values():
        if inspect.isclass(v):
            top_level_cls_name = v.__qualname__.split(".")[0]
            if v.__module__ == func_module_name:
                # v is a class defined in the same module as UDF func's, need to dynamically parse the class
                # if v is class defined in cls, then we should not re-generate code for the nested class
                inferred_classes.append(
                    (v, v.__qualname__.startswith(top_level_cls_name))
                )
            else:
                # v is a class defined in another module, import the top level class from another module
                ref_objects[top_level_cls_name] = v
        elif inspect.isfunction(v) or isinstance(v, classmethod):
            # v is a function/classmethod, get the references objects of the Function object
            get_func_references(
                v if not isinstance(v, classmethod) else v.__func__, ref_objects
            )
        else:
            # cls.__dict__ would also return __module__, __doc__, __weakref__ which are not required
            # for code generation, however, class variables is also included in __dict__, we don't do value evaluation
            # for them in the current implementation (e.g. the declaration of class variables is assigned the
            # result of function call). But we shall introduce an argument to control the behavior,
            # check JIRA SNOW-649884
            pass

    # recursively handling inferred classes that should be analyzed dynamically
    for inferred_class, is_nested_class in inferred_classes:
        get_class_references(
            inferred_class,
            func,
            ref_objects,
            classes_to_generate,
            generate_code_for_class=not is_nested_class,
        )


def extract_func_global_refs(code: CodeType) -> Dict[str, None]:
    # inspired by cloudpickle to recursively extract all the global references used by the target func's code object
    co_names = code.co_names
    out_names = {}
    for instr in dis.get_instructions(code):
        op = instr.opcode
        if op in GLOBAL_OPS:
            out_names[co_names[instr.arg]] = None

    if code.co_consts:
        for const in code.co_consts:
            if isinstance(const, CodeType):
                out_names.update(extract_func_global_refs(const))

    return out_names


def remove_function_udf_annotation(udf_source_code: str) -> str:
    """
    Remove the udf/pandas_udf annotation to avoid re-registration.
    """
    res = re.search(r"@(pandas_)?udf", udf_source_code)
    if res is None:
        return udf_source_code
    udf_anno_begin = res.start()
    udf_anno_end = res.end()
    if udf_source_code[udf_anno_end] == "\n":
        # just @udf
        return udf_source_code[udf_anno_end + 1 :]
    elif udf_source_code[udf_anno_end] != "(":
        # not a @udf
        return udf_source_code

    udf_anno_end = udf_anno_end + 1
    parenthesis_count = 1

    # find the pairing ')' of the leading 'udf('
    while parenthesis_count != 0:
        if udf_source_code[udf_anno_end] == "(":
            parenthesis_count += 1
        elif udf_source_code[udf_anno_end] == ")":
            parenthesis_count -= 1
        udf_anno_end += 1

    # check if there are still @udf annotations, then it's a
    code_after_remove = f"{udf_source_code[:udf_anno_begin].strip()}\n{udf_source_code[udf_anno_end:].strip()}".strip()
    if re.search(r"@(pandas_)?udf", code_after_remove) is not None:
        raise ValueError("An UDF can not be registered more than once.")
    return code_after_remove


def check_func_type(func: Any) -> None:
    """
    Check whether the target function is a valid type for source code generation. Raise error if not supported.
    """
    if (
        isinstance(func, classmethod)
        or inspect.ismethod(func)
        or not (isinstance(func, (FunctionType, BuiltinFunctionType)))
    ):
        error_msg = f"Code generation for {type(func)} is not supported yet."
        logger.debug(error_msg)
        raise TypeError(error_msg)


def generate_source_code(
    func: Union[FunctionType, BuiltinFunctionType], code_as_comment: bool = True
) -> str:
    """
    Dynamically generate source code of the given Python functions including:
      - The function itself
      - The functions/classes that are defined and referenced by the target function in the same module
      - The modules/class/method that have to be imported as used by the target function
      - The global or closure variables used by the target function

    The current implementation locks the support for the following scenarios:
     - Decorated functions: https://snowflakecomputing.atlassian.net/browse/SNOW-644983
     - Method and classmethod: https://snowflakecomputing.atlassian.net/browse/SNOW-644984

    Args:
        func: The target function to generate source code for.
        code_as_comment: Whether the code will be generated as comment.

    Returns:
        The generated source code.
    """

    try:
        check_func_type(func)
    except TypeError:
        if code_as_comment:
            # if it is an unsupported type, then no code generation and return empty string
            return ""
        raise

    # stored referenced object, key is the object name, value is the object
    ref_objects: Dict[str, Any] = {}
    # stored modules, each item should be a tuple of two strings, first is the true module name, second is the used name
    # such as alias or just the name
    to_import: Set[Tuple[str, str]] = set()
    # imports class/funcs/vars form other modules, each key is the module name
    # each item is a set of tuples of two strings as the to_import, first module name, second alias
    to_import_from_module: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)
    # classes that should be generated source code
    classes_to_generate: List[type] = []

    header_text = CODE_AS_COMMENT_HINT if code_as_comment else CODE_HEADER
    classes_text = ""

    # 1. find objects referenced by functions including classes, methods, modules, global variables
    find_target_func_objects_references(
        func, to_import, ref_objects, classes_to_generate
    )

    # 2. deal with the referenced objects by types
    func_text, global_vars_text = resolve_target_func_referenced_objects_by_type(
        func, to_import, to_import_from_module, ref_objects, code_as_comment
    )

    # 3. deal with the classes defined in the same module as func's
    for cls in classes_to_generate:
        classes_text = f"{classes_text}{textwrap.dedent(inspect.getsource(cls))}"

    # 4. deal with imports and alias
    imports_str = resolve_target_func_imports(to_import, to_import_from_module)

    # concatenating all the referenced parts
    source_code_without_target_func = f"{header_text}{imports_str}{global_vars_text}{classes_text}{func_text}".rstrip()

    # 5. handle func, remove the udf annotation
    complete_source_code, func_assignment = handle_target_func_self_source_code(
        func, source_code_without_target_func, code_as_comment
    )

    # 6. handle function assignment
    complete_source_code = f"""\
{complete_source_code}
{f"func = {func_assignment}"}\
""".strip()

    # 7. if code as comment is true, prefix each line with '#'
    if code_as_comment:
        complete_source_code = comment_source_code(complete_source_code)

    return complete_source_code.strip()


def is_lambda(func: FunctionType) -> bool:
    """
    Check whether the target function is a lambda function.
    """
    return func.__name__ == "<lambda>"


def get_lambda_code_text(code_text: str) -> str:
    """
    Extract the lambda expression from code text.

    Args:
        The original code text containing the lambda expression.

    Returns:
        The string of the lambda expression.

    """
    # add a wrapper to handle the case that the line of lambda source code does not include caller
    # such that ast could parse the expression tree:
    #     session.udf.register(
    #         lambda x, y: x + y, ...
    #     )
    try:
        source_ast = ast.parse(code_text)
    except SyntaxError as exc:
        if "cannot assign to lambda" in str(exc):
            # handle case like:
            # session.udf.register(
            #    lambda x, y: x + y, ...
            # )
            code_text = f"wrapper({code_text})"
        elif "unmatched ')'" in str(exc):
            # handle case like:
            # session.udf.register(
            #    lambda x, y: x + y, ...)
            code_text = f"wrapper({code_text}"
        source_ast = ast.parse(code_text)
    lambda_node = next(
        (node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)), None
    )
    if not lambda_node:
        raise TypeError("lambda function can not be extracted")

    lines = code_text.splitlines()
    # single line lambda
    if len(lines) == 1:
        return code_text[lambda_node.col_offset : lambda_node.end_col_offset]

    lambda_code_text = ""
    # lambda of multiple lines
    # handle case like:
    # session.udf.register(
    #    lambda x, y:\
    #    x + y, ...)
    for line_idx in range(lambda_node.lineno - 1, lambda_node.end_lineno):
        line = lines[line_idx]
        if line_idx == 0:
            lambda_code_text = f"{lambda_code_text}{line[lambda_node.col_offset:]}\n"
        elif line_idx == lambda_node.end_lineno - 1:
            lambda_code_text = f"{lambda_code_text}{line[: lambda_node.end_col_offset]}"
        else:
            lambda_code_text = f"{lambda_code_text}{line}\n"
    return lambda_code_text.strip()


def extract_submodule_imports(
    func: FunctionType, top_level_modules: Iterable[ModuleType]
) -> Set[Tuple[str, str]]:
    """
    Get submodule imports, the func code co_names only gives the top level module names, the submodule imports
    have to be inferred manually. Consider the following example:

    import a1.a2.a3.a4
    def func():
        a1.a2.a3.a4.foo()

    func.__code__.co_names only contains ("a1", "a2", "a3", "a4", "foo") which does not include the
    complete import path information.

    To reconstruct "a1.a2.a3.a4", the current strategy is to import each prefix import of the import chains.
    This is not a perfect solution as we could import modules not used, but it works.

    Args:
        func: The target function to generate source code for.
        top_level_modules: The name of top level modules from which to search the referenced imported objects
            or submodules.

    Returns:
        A set of tuple with each tuple composed of two string, the first one is actual name for the imported object,
        and the second one is alias.

    """
    imports = set()
    for module in top_level_modules:
        module_prefix = f"{module.__name__}."
        for name in [m for m in sys.modules if m.startswith(module_prefix)]:
            tokens = set(name[len(module_prefix) :].split("."))
            if not tokens - set(
                func.__code__.co_names
            ):  # only add imports that co_names contains
                imports.add((name, name))
    return imports


def find_target_func_objects_references(
    func: Union[FunctionType, BuiltinFunctionType],
    to_import: Set[Tuple[str, str]],
    ref_objects: Dict[str, Any],
    classes_to_generate: List[type],
) -> None:
    """
    Find objects referenced by functions including classes, methods, modules, global variables.

    Args:
        func: The target function to generate source code for.
        to_import: set of name and alias pairs of direct imports which should be generated as "import xxx"
            or "import xxx as yyy".
        ref_objects: dict of objects referenced by the target function, key is the name and value is the object.
        classes_to_generate: list of classes which are defined in the same module as the target function.
            Code generation is required for these classes.
    """
    func_module_name = func.__module__
    if isinstance(func, FunctionType):
        get_func_references(func, ref_objects)
        to_import.update(
            extract_submodule_imports(
                func, [v for v in ref_objects.values() if isinstance(v, ModuleType)]
            )
        )
        for v in [
            v
            for v in ref_objects.values()
            if inspect.isclass(v) and v.__module__ == func_module_name
        ]:
            get_class_references(v, func, ref_objects, classes_to_generate)
    elif isinstance(func, BuiltinFunctionType):
        if func_module_name != builtins.__name__:
            to_import.add((func_module_name, func_module_name))


def resolve_target_func_referenced_objects_by_type(
    func: Union[FunctionType, BuiltinFunctionType],
    to_import: Set[Tuple[str, str]],
    to_import_from_module: Dict[str, Set[Tuple[str, str]]],
    ref_objects: Dict[str, Any],
    code_as_comment: bool,
) -> Tuple[str, str]:
    """
    Deal with the referenced objects by types, handles modules/classes/methods/global variables, generate source code
    for referenced functions defined in the same module as the target function's and referenced variables.

    Args:
        func: The target function to generate source code for.
        to_import: set of name and alias pairs of direct imports which should be generated as "import xxx"
            or "import xxx as yyy".
        to_import_from_module: dict of import information, key is the module name with value being the set of
            name and alias paris of imported objects which should be generated as "from xxx import yyy" or
            "from xxx import yyy as zzz".
        ref_objects: dict of objects referenced by the target function, key is the name and value is the object.
        code_as_comment: Whether the code will be generated as comment.

    Returns:
        A tuple of two strings, the first one is the source code of referenced functions defined in the same module
        as the target function's, and the second is the source code of referenced variables.
    """
    func_module_name = func.__module__
    func_text = ""
    global_vars_text = ""
    for name, obj in ref_objects.items():
        if obj == func:
            continue
        if inspect.ismodule(obj):
            # a) imported modules
            to_import.add((obj.__name__, name))  # name could be an alias
        elif (
            inspect.isclass(obj) or inspect.isfunction(obj)
        ) and obj.__module__ != func_module_name:
            # b) classes or functions imported from other modules
            to_import_from_module[obj.__module__].add(
                (obj.__name__, name)
            )  # name could be an alias
        else:
            # function/class/variables defined in the same module
            if inspect.isfunction(obj):
                func_text = f"{func_text}{textwrap.dedent(inspect.getsource(obj))}"
            elif inspect.isclass(obj):
                # dynamic class parsing will be handled separately
                pass
            else:
                # c) global variables used by UDF
                if code_as_comment:
                    global_vars_text = (
                        f"{global_vars_text}{name}  # variable of type {type(obj)}\n"
                    )
                    continue  # skip the serialization part if we just need code as comment
                # v does not have source code, then it's a global variable of which the value has been evaluated
                try:
                    global_vars_text = f"""\
{global_vars_text}
{name} = pickle.loads(bytes.fromhex('{pickle.dumps(obj).hex()}'))  \
# {name} is of type {type(obj)} and serialized by snowpark-python
"""
                except Exception as exc:
                    logger.debug(
                        f"Unable to generate source code for object {name} of type {type(obj)} due to exception {exc}"
                    )
                    raise
    return func_text, global_vars_text


def resolve_target_func_imports(
    to_import: Set[Tuple[str, str]],
    to_import_from_module: Dict[str, Set[Tuple[str, str]]],
) -> str:
    """
    Deal with imports and alias, generate imports string.

    Args:
        to_import: set of name and alias pairs of direct imports which should be generated as "import xxx"
            or "import xxx as yyy".
        to_import_from_module: dict of import information, key is the module name with value being the set of
            name and alias paris of imported objects which should be generated as "from xxx import yyy" or
            "from xxx import yyy as zzz".

    Returns:
        A string of generated imports.
    """
    imports = [
        f"import {name + ' as ' if name != alias else ''}{alias}"
        for name, alias in sorted(to_import)
    ]
    for module, name_alias_pairs in sorted(to_import_from_module.items()):
        classes = ", ".join(
            f"{name + ' as ' if name != alias else ''}{alias}"
            for name, alias in sorted(name_alias_pairs)
        )
        imports.append(f"from {module} import {classes}")
    return "\n".join(imports) + ("\n" if imports else "")


def handle_target_func_self_source_code(
    func: FunctionType, source_code_without_target_func: str, code_as_comment: bool
) -> Tuple[str, str]:
    """
    Generate the source code of the target func itself and apply function assignment.

    Args:
        func: The target function to generate source code for.
        source_code_without_target_func: The generated code without the target function. The target function code
            and function assignment will be appended to this one.
        code_as_comment: Whether the code will be generated as comment.

    Returns:
        A tuple of two strings, the first one is the complete source code including target functions and all of its
        referenced objects, and the second one is function assignment.
    """
    func_module_name = func.__module__
    complete_source_code = source_code_without_target_func
    if isinstance(func, FunctionType):
        func_source_code = textwrap.dedent(inspect.getsource(func))
        if not code_as_comment:
            func_source_code = remove_function_udf_annotation(func_source_code)
        if not is_lambda(func):
            complete_source_code = f"{complete_source_code}\n{func_source_code}"
        func_assignment = (
            get_lambda_code_text(func_source_code) if is_lambda(func) else func.__name__
        )
    else:
        # BuiltinFunctionType
        func_assignment = (
            func.__name__
            if func_module_name == builtins.__name__
            else f"{func_module_name}.{func.__name__}"
        )
    return complete_source_code, func_assignment


def comment_source_code(complete_source_code: str) -> str:
    """
    Prefix each line in source code with '#'

    Args:
        complete_source_code: The complete source code including target functions and all of its
        referenced objects

    Returns:
        The complete source code string with each line prefixed with "#".
    """
    return "\n".join(
        [f"#{f' {line}' if line else ''}" for line in complete_source_code.splitlines()]
    )
