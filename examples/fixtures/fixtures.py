def write_external_csv_file(data, bucket_name, file_name):
    from gcp_tools import Storage

    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "income": [50000.0, 20000.0, 30000.0],
        "is_student": [False, True, False],
    }
    file_path = f"{bucket_name}/{file_name}"

    Storage(bucket_name).create()
    txt_data = "\n".join([",".join(map(str, row)) for row in zip(*data.values())])
    with Storage(file_path).open(mode="w") as f:
        f.write(txt_data)
