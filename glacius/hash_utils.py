import hashlib


def md5_hash_str(input_str: str) -> str:
    md5_hash = hashlib.md5()
    md5_hash.update(input_str.encode("utf-8"))
    return md5_hash.hexdigest()
