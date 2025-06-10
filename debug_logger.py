import functools
import traceback

def debug_log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(f"[DEBUG] >>> Calling: {func.__name__}")
        print(f"[DEBUG] >>> Args: {args}, Kwargs: {kwargs}")
        try:
            result = func(*args, **kwargs)
            print(f"[DEBUG] <<< Success: {func.__name__} returned: {result}")
            return result
        except Exception as e:
            print(f"[ERROR] !!! Exception in {func.__name__}: {str(e)}")
            traceback.print_exc()
            return {
                "type": "error",
                "action": func.__name__,
                "error": str(e)
            }
    return wrapper
