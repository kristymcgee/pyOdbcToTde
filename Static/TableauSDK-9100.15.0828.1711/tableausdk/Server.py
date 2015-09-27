# -----------------------------------------------------------------------
# Copyright (c) 2012 Tableau Software, Incorporated
#                    and its licensors. All rights reserved.
# Protected by U.S. Patent 7,089,266; Patents Pending.
#
# Portions of the code
# Copyright (c) 2002 The Board of Trustees of the Leland Stanford
#                    Junior University. All rights reserved.
# -----------------------------------------------------------------------
# Server.py
# -----------------------------------------------------------------------
# WARNING: Computer generated file.  Do not hand modify.

from ctypes import *
from . import Exceptions
from . import Libs
from . import StringUtils
from . import Types

libs = Libs.LoadLibs()
server_lib = libs.load_lib('Server')

class ServerConnection(object):
    """Represents a connection to an instance of Tableau Server."""

    def __init__(
        self
    ):
        """Initializes a new instance of the ServerConnection class."""

        self._handle = c_void_p(None)

        ret = server_lib.TabServerConnectionCreate(
            byref(self._handle)
        )

        if int(ret) != int(Types.Result.SUCCESS):
            raise Exceptions.TableauException(ret, Exceptions.GetLastErrorMessage())

    def close(self):
        """Destroys a server connection object."""
        if self._handle != None:
            server_lib.TabServerConnectionClose( self._handle )
            self._handle = None

    def __del__(self):
        self.close()

    def setProxyCredentials(
        self
      , username
      , password
    ):
        """Sets the username and password for the HTTP proxy. This method is needed only if the server connection is going through a proxy that requires authentication."""

        if username == None:
            raise ValueError('username must not be None')

        if password == None:
            raise ValueError('password must not be None')

        result = server_lib.TabServerConnectionSetProxyCredentials(
        self._handle
          , StringUtils.ToTableauString(username)
          , StringUtils.ToTableauString(password)
        )

        if result != Types.Result.SUCCESS:
            raise Exceptions.TableauException(result, Exceptions.GetLastErrorMessage())

    def connect(
        self
      , host
      , username
      , password
      , siteID
    ):
        """Connects to the specified server and site."""

        if host == None:
            raise ValueError('host must not be None')

        if username == None:
            raise ValueError('username must not be None')

        if password == None:
            raise ValueError('password must not be None')

        if siteID == None:
            raise ValueError('siteID must not be None')

        result = server_lib.TabServerConnectionConnect(
        self._handle
          , StringUtils.ToTableauString(host)
          , StringUtils.ToTableauString(username)
          , StringUtils.ToTableauString(password)
          , StringUtils.ToTableauString(siteID)
        )

        if result != Types.Result.SUCCESS:
            raise Exceptions.TableauException(result, Exceptions.GetLastErrorMessage())

    def publishExtract(
        self
      , path
      , projectName
      , datasourceName
      , overwrite
    ):
        """Publishes a data extract to the server."""

        if path == None:
            raise ValueError('path must not be None')

        if projectName == None:
            raise ValueError('projectName must not be None')

        if datasourceName == None:
            raise ValueError('datasourceName must not be None')

        result = server_lib.TabServerConnectionPublishExtract(
        self._handle
          , StringUtils.ToTableauString(path)
          , StringUtils.ToTableauString(projectName)
          , StringUtils.ToTableauString(datasourceName)
          , c_bool(overwrite)
        )

        if result != Types.Result.SUCCESS:
            raise Exceptions.TableauException(result, Exceptions.GetLastErrorMessage())

    def disconnect(
        self
    ):
        """Disconnects from the server."""

        result = server_lib.TabServerConnectionDisconnect(
        self._handle
        )

        if result != Types.Result.SUCCESS:
            raise Exceptions.TableauException(result, Exceptions.GetLastErrorMessage())

class ServerAPI(object):
    """Provides management functions for the Server API."""

    @staticmethod
    def initialize(
    ):
        """Initializes the Server API. You must initialize the API before you call any methods in the ServerConnection class."""

        result = server_lib.TabServerAPIInitialize(
        )

        if result != Types.Result.SUCCESS:
            raise Exceptions.TableauException(result, Exceptions.GetLastErrorMessage())

    @staticmethod
    def cleanup(
    ):
        """Shuts down the Server API. You must call this method after you have finished calling other methods in the Server API."""

        result = server_lib.TabServerAPICleanup(
        )

        if result != Types.Result.SUCCESS:
            raise Exceptions.TableauException(result, Exceptions.GetLastErrorMessage())
