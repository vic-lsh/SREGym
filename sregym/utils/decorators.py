import functools


def mark_fault_injected(method):
    # ``__wrapped__`` lets a composite call the undecorated recovery and see its errors.
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            result = method(self, *args, **kwargs)
        except Exception as e:
            if method.__name__ == "inject_fault":
                # We exit if there's an error during fault injection, warning if in recovery
                raise
            else:
                print(f"[{method.__name__}] Warning: encountered error: {e!r}")
                result = None

        self.fault_injected = method.__name__ == "inject_fault"
        return result

    return wrapper
