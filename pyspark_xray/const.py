import os
from sys import platform

if os.name=="nt" or platform=="darwin":    #platofmr==darwin is mac
    CONST_BOOL_LOCAL_MODE=True
else:
    CONST_BOOL_LOCAL_MODE=False