import threading

# this file is modeled after sql_counter.py in snowpark pandas API.

AST_ENCODER_CALLED = "ast_encoder_called"

def mark_ast_encoder_called():
    threading.current_thread().__dict__[AST_ENCODER_CALLED] = True


def clear_ast_encoder_called():
    threading.current_thread().__dict__[AST_ENCODER_CALLED] = False

def is_ast_encoder_called():
    if AST_ENCODER_CALLED in threading.current_thread().__dict__:
        return threading.current_thread().__dict__.get(AST_ENCODER_CALLED)
    return False