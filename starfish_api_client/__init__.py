"""
The ``starfish_query_client`` is an unofficial Python client for executing
queries against the `Starfish API server <https://starfishstorage.com/>`_.


Usage
-----

The ``StarfishServer`` class is used to handle API authentication and query
submission. Start by instantiating a new server instance and authenticating
with an appropriate username and password.

.. code-block:: python

   >>> from starfish_api_client import StarfishServer

   >>> # Make sure to use the URL for Starfish API and not the root Starfish server
   >>> # The API url typically ends in `/api`
   >>> api_server = StarfishServer(api_url='https://my.api.com/api')
   >>> api_server.authenticate('username', 'password')

Once authenticated, the server instance provides limited capabilities for
fetching available volume names and directories.

.. code-block:: python

   >>> # Use the `get_volume_names` method to fetch available volumes
   >>> volumes = api_server.get_volume_names()
   >>> print(volumes)

   [ 'volume1', 'volume2', ... ]

   >>> # Use the `get_subpaths` method to fetch relative directories
   >>> # under a given volume/path
   >>> paths = api_server.get_subpaths('volume1')
   >>> print(paths)

   [ 'directory1', 'directory2', ... ]

More complex queries are handled by the ``submit_query`` method. Valid arguments
include all arguments supported by the Starfish asynchronous API endpoint
(see the official Starfish API dcs for more details).

.. code-block:: python

   >>> query = server.submit_query(
   ...     volume="crc-ihome/astar",
   ...     size_unit="KiB",
   ...     limit=2,
   ...     ...  # other query parameters
   ... )
   ... print(query.get_result())

When submitting a large number of simultanepous queries, the
``get_result_async`` coroutine can be used as a drop in replacement for the
synchronous `get_result` method.

.. code-block:: python

   >>> asyncio.gather(
   ...     api_server.submit_query(...).get_result_async(),
   ...     api_server.submit_query(...).get_result_async(),
   ...     api_server.submit_query(...).get_result_async()
   ... )
"""

import importlib.metadata

from .client import *

try:
    __version__ = importlib.metadata.version('quota-notifier')

except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    __version__ = '0.0.0'
