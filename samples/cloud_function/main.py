from function import square_root


def entry_point(request):
    data = request.get_json()
    if data is None:
        data = request.form
    return square_root(data)
