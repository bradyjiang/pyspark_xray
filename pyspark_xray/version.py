def _safe_int(string):
    """Simple function to convert strings into ints without dying.
    Helps when we define versions like 0.1.0dev"""
    try:
        return int(string)
    except ValueError:
        return string


__version__ = '0.1.0'
VERSION = tuple(_safe_int(x) for x in __version__.split('.'))