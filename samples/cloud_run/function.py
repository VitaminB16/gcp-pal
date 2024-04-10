from math import sqrt


def square_root(data):
    print(f"Received data: {data}")
    print(f"Square root of data: {sqrt(data)}")
    return {"result": sqrt(data)}
