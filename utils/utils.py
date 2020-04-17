def chunks(arr, size):
    result = []
    for i in range(0, len(arr), size):
        chunk = arr[i:i + size]
        result.append(chunk)
    return result
