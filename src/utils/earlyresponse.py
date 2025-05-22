"""
Error class for returning an early response from a Flask view, i.e. a template
"""

class EarlyResponse(Exception):
    """
    Constructor: raise this exception to return a template
    """
    def __init__(self, response):
        self.response = response;


