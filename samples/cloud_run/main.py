import json
from flask import Flask, request as flask_request

from function import square_root


app = Flask(__name__)


def entry_point(data):
    print(f"Data: {data}")
    print(f"Type: {type(data)}")
    if isinstance(data, dict):
        data = data.get("data")
    data = float(data)
    return square_root(data)


@app.route("/", methods=["POST", "GET"])
def flask_entry_point():
    if flask_request.method == "POST":
        data = flask_request.get_json()
        data = json.loads(data)
        return entry_point(data=data)
    else:
        return "Hello, World!"


if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")
    app.run(host=host, port=port)
