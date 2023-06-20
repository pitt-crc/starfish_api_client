"""The ``client`` module provides two classes for interacting with the
Starfish API.  The ``StarfishServer`` class is responsible for handling client
authentication and query submission. Submitted queries are returned as
``AsyncQuery`` objects which monitor the submitted query status and return
query results as they become available.

Module Contents
---------------
"""

import asyncio
import logging
import ssl
import urllib.parse

import aiohttp
import requests

logger = logging.getLogger('starfish_api_client')


class AsyncQuery:
    """An asynchronous query submitted to a Starfish API server"""

    def __init__(self, api_url: str, headers: dict[str, str], query_id: str, verify=True) -> None:
        """Instantiate a new query instance

        Args:
            api_url: The base API server URL
            headers: Header values to use when polling for query results
            query_id: The ID of the submitted query
            verify: Require successful SSL verification
        """

        self._api_url = api_url
        self._headers = headers
        self._query_id = query_id
        self._result = None

        self._ssl_context = ssl.SSLContext()
        self._ssl_context.verify_mode = ssl.CERT_REQUIRED if verify else ssl.CERT_NONE

    @property
    def query_id(self) -> str:
        """Return the ID of the submitted query"""

        return self._query_id

    async def _check_query_result_ready(self) -> bool:
        """Check if the query result is ready for consumption

        Returns:
            The query completion state as a boolean
        """

        query_status_url = urllib.parse.urljoin(self._api_url, f'async/query/{self.query_id}')

        logging.debug(f'Polling query result for query {self.query_id} ...')
        async with aiohttp.ClientSession() as session:
            async with session.get(query_status_url, headers=self._headers, ssl=self._ssl_context) as response:
                response.raise_for_status()
                json = await response.json()
                return json["is_done"]

    async def _get_query_result(self) -> dict:
        """Fetch query results from the API

        This method assumes the caller has already checked the query result is
        has been prepared by the API and is ready for consumption.

        Returns:
            The JSON query result
        """

        query_result_url = urllib.parse.urljoin(self._api_url, f'async/query_result/{self.query_id}')

        logging.debug(f'Fetching query result for query {self.query_id} ...')
        async with aiohttp.ClientSession() as session:
            async with session.get(query_result_url, headers=self._headers, ssl=self._ssl_context) as response:
                response.raise_for_status()
                self._result = await response.json()
                return self._result

    async def get_result_async(self, polling: int = 3) -> dict:
        """Return the query result as soon as it is ready

        This method is intended for asynchronous use. See the ``get_result``
        method for a synchronous version of the method.

        Args:
            polling: Frequency in seconds to poll the API server for query results
        """

        logging.info(f'Checking query result for query {self.query_id} ...')
        if self._result is not None:
            logging.debug(f'Query {self.query_id} is already cached')
            return self._result

        while True:
            if await self._check_query_result_ready():
                logging.debug(f'Query {self.query_id} is ready')
                return await self._get_query_result()

            await asyncio.sleep(polling)

    def get_result(self, polling: int = 3) -> dict:
        """Return the query result as soon as it is ready

        This method is intended for synchronous use. See the ``get_result_async``
        method for an asynchronous version of the method.

        Args:
            polling: Frequency in seconds to poll the API server for query results
        """

        return asyncio.run(self.get_result_async(polling=polling))


class StarfishServer:
    """Class for interacting with a Starfish API server."""

    def __init__(self, api_url: str, verify=True) -> None:
        """Initialize a new Server instance

        Args:
            api_url: The Starfish API URL, typically ending in /api/
            verify: Require successful SSL verification
        """

        self.api_url = api_url
        self._token = None
        self.verify = verify

    def _get_headers(self) -> dict:
        """Return headers to include when submitting API requests

        This method requires the parent instance to be authenticated against the API server.

        Returns:
            A dictionary with request headers

        Raises:
            RuntimeError: If the parent instance is not already authenticate.
        """

        if self._token is None:
            raise RuntimeError('Server is not authenticated')

        return {
            "accept": "application/json",
            "Authorization": "Bearer {}".format(self._token),
        }

    def authenticate(self, username: str, password: str) -> None:
        """Authenticate against the Starfish API

        Args:
            username: Authentication username
            password: Authentication password

        Raises:
            HTTPError: When the authentication request errors out or is unsuccessful
        """

        auth_url = urllib.parse.urljoin(self.api_url, 'auth/')
        payload = {"username": username, "password": password}

        logger.info(f'Authenticating against server {self.api_url} ...')
        response = requests.post(auth_url, json=payload, verify=self.verify)
        response.raise_for_status()

        logging.debug('Authentication successful')
        self._token = response.json()["token"]

    def get_volume_names(self) -> list[str]:
        """Return a list of volume names accessible via the API server

        Returns:
            A list of volume names returned by the API
        """

        storage_url = urllib.parse.urljoin(self.api_url, 'storage/')

        logger.info('Fetching volume names from server...')
        response = requests.get(storage_url, headers=self._get_headers(), verify=self.verify)
        response.raise_for_status()
        return [item["name"] for item in response.json()["items"]]

    def get_subpaths(self, volume_and_paths: str) -> list[str]:
        """Return a list of top level directories located under the given volume path

        Args:
            volume_and_paths: Name of the volume and path in ``volume:path`` format

        Returns:
            A list of directory names as strings
        """

        storage_url = urllib.parse.urljoin(self.api_url, f'storage/{volume_and_paths}')

        logger.info(f'Fetching paths from server under {volume_and_paths} ...')
        response = requests.get(storage_url, headers=self._get_headers(), verify=self.verify)
        response.raise_for_status()
        return [item["Basename"] for item in response.json()["items"]]

    async def submit_query_async(self, **kwargs) -> AsyncQuery:
        """Submit a new API query

        Valid arguments include all query-string parameters supported by the
        API ``query`` endpoint. See the official Starfish API documentation
        for more details.

        Returns:
            A ``StarfishQuery`` instance representing the submitted query
        """

        query_url = urllib.parse.urljoin(self.api_url, 'async/query/')

        logging.info('Submitting new API query ...')
        response = requests.post(query_url, params=kwargs, headers=self._get_headers(), verify=self.verify)
        response.raise_for_status()
        query_id = response.json()["query_id"]

        logging.debug(f'Query returned with id {query_id}')
        return AsyncQuery(self.api_url, self._get_headers(), query_id, verify=self.verify)
