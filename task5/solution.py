from functools import wraps
import time
from multiprocessing import Process, Pipe

def timer(func):
    @wraps(func)
    def wrapper(*args,**kwargs):
        start_time = time.perf_counter()
        result = func(*args,**kwargs)
        end_time = time.perf_counter()
        print(f"{end_time-start_time:.6f} - время вычисления")
        return result
    return wrapper

def function_first(x):
    return x**2 - x**2 +x*4 - x*5 + x + x

def function_second(x):
    return x+x
@timer
def hard_computing(conn, func, runs, value):
    for i in range(runs):
        value = func(value)
    conn.send(value)
    conn.close()

def multi_computing(runs, value):
    parent_conn1, child_conn1 = Pipe()
    parent_conn2, child_conn2 = Pipe()

    p1 = Process(target=hard_computing, args=(child_conn1, function_first, runs, value))
    p2 = Process(target=hard_computing, args=(child_conn2, function_second, runs, value))

    # Запуск процессов
    p1.start()
    p2.start()
    # Получение результатов
    results1 = parent_conn1.recv()
    results2 = parent_conn2.recv()

    # Ожидание завершения процессов
    p1.join()
    p2.join()

    # Вычисление по формуле 3
    start_time = time.perf_counter()
    results3 = results1 + results2
    end_time = time.perf_counter()
    print(f"{end_time-start_time:.6f}")
    return results3
if __name__ == '__main__':
    a = 10;
    print("first run")
    multi_computing(10000, a);
    print("second run")
    multi_computing(100000, a);
