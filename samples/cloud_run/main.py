import json
from flask import Flask, request as flask_request

from function import square_root


app = Flask(__name__)


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


@app.route("/", methods=["POST", "GET"])
def flask_entry_point():
    if flask_request.method == "POST":
        payload = flask_request.get_json()
        return entry_point(payload)
    else:
        return "Hello, World!"


if __name__ == "__main__":
    app.run(port=8080)
