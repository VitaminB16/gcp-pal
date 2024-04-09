import json
from function import square_root


def entry_point(request):
    data = request.get_json()
    if data is None:
        data = request.form
    data = json.loads(data)
    print(f"Data: {data}")
    print(f"Type: {type(data)}")
    if isinstance(data, dict):
        data = data.get("data")
    data = float(data)
    return square_root(data)
