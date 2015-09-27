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

from tableausdk import *
from tableausdk.Server import *

try:
    # Initialize Tableau Server API
    ServerAPI.initialize()

    # Create the server connection object
    serverConnection = ServerConnection()

    # Connect to the server
    serverConnection.connect('http://localhost', 'username', 'password', 'siteID');

    # Publish order-py.tde to the server under the default project with name Order-py
    serverConnection.publishExtract('order-py.tde', 'default', 'Order-py', False);

    # Disconnect from the server
    serverConnection.disconnect();

    # Destroy the server connection object
    serverConnection.close();

    # Clean up Tableau Server API
    ServerAPI.cleanup();

except TableauException, e:
    print 'Something bad happened:', e
