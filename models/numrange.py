class NumRange(object):

    """ Class for number range nodes    

    Attributes        
        min                 the bottom of the range                
        max                 the top of the range
    """

    def __init__(self, min: int, max: int):        
        self.min = min
        self.max = max
        self.range = max - min
        self.value = f'{min},{max}'
