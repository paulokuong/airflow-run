from functools import wraps
import time


def retry(max_retries, message=''):
    def retry_fn_sub_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kargs):
            success = False
            err_msg = ''
            retry = 0
            fib_num_a = 1
            fib_num_b = 1

            while success is False and retry <= max_retries:
                if message:
                    print(message)
                try:
                    response = fn(*args, **kargs)
                    success = True
                except Exception as err:
                    err_msg = str(err)
                    success = False
                new_interval = fib_num_b + fib_num_a
                fib_num_a = fib_num_b
                time.sleep(new_interval)
                fib_num_b = new_interval
                retry += 1

            if retry > max_retries:
                raise Exception(err_msg)
            return response

        return wrapper

    return retry_fn_sub_decorator
