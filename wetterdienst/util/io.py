def read_in_chunks(file_object, chunk_size=1024):
    """
    Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k.

    -- https://stackoverflow.com/questions/519633/lazy-method-for-reading-big-file-in-python/519653#519653  # Noqa: E501, B950
    """
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data
