# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# Unlicensed use of the contents of this file is prohibited. Please refer to
# the NOTICES.txt file for further details.
#
# -----------------------------------------------------------------------------

import ctypes
from . import Libs

libs = Libs.LoadLibs()
common_lib = libs.load_lib('Common')

def ToTableauString(str):
    """Convert a Python string to a C TableauString"""

    wstr = ctypes.c_wchar_p(unicode(str))
    buffer = ctypes.create_string_buffer(ctypes.sizeof(ctypes.c_wchar) * (len(str) + 1))
    common_lib.ToTableauString(wstr, ctypes.byref(buffer))
    return buffer

def FromTableauString(ts):
    """Convert a C TableauString to a Python string"""

    tslen = common_lib.TableauStringLength(ts)
    buffer = ctypes.create_string_buffer((tslen + 1) * ctypes.sizeof(ctypes.c_wchar))
    common_lib.FromTableauString(ts, ctypes.byref(buffer))
    return ctypes.wstring_at(buffer, tslen)
