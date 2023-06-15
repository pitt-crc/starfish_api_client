import asyncio
import logging

import requests

logger = logging.getLogger('starfish_client')


class AsyncQuery:
    """An asynchronous query submitted to a Starfish API server"""

    def __init__(self, api_url: str, headers: dict[str, str], query_id: str) -> None:
        """Instantiate a new query instance

        Args:
            api_url: The base API server URL
            headers: Header values to use when polling for query results
            query_id: The ID of the submitted query
        """

        self._api_url = api_url
        self._headers = headers
        self._query_id = query_id
        self._result = None

    @property
    def query_id(self) -> str:
        """Return the ID of the submitted query"""

        return self._query_id

    async def _check_query_result_ready(self) -> bool:
        """Check if the query result is ready for consumption

        Returns:
            The query completion state as a boolean
        """

        query_status_url = self._api_url + "async/query/" + self.query_id
        status_response = requests.get(query_status_url, self._headers)
        status_response.raise_for_status()
        return status_response.json()["is_done"]

    async def _get_query_result(self) -> dict:
        """Fetch query results from the API

        This method assumes the caller has already checked the query result is
        has been prepared by the API and is ready for consumption.

        Returns:
            The JSON query result
        """

        query_result_url = self._api_url + "async/query_result/" + self.query_id
        response = requests.get(query_result_url, self._headers)
        response.raise_for_status()
        self._result = response.json()
        return self._result

    async def get_result_async(self, polling: int = 3) -> dict:
        """Return the query result as soon as it is ready

        This method is intended for asynchronous use. See the ``get_result``
        method for a synchronous version of the method.

        Args:
            polling: Frequency in seconds to poll the API server for query results
        """

        if self._result is not None:
            return self._result

        while True:
            if await self._check_query_result_ready():
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

    def __init__(self, api_url: str) -> None:
        """Initialize a new Server instance

        Args:
            api_url: The Starfish API URL, typically ending in /api/
        """

        self.api_url = api_url
        self._token = None

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

        auth_url = self.api_url + "auth/"
        payload = {"username": username, "password": password}
        response = requests.post(auth_url, json=payload)
        response.raise_for_status()
        self._token = response.json()["token"]

    def get_volume_names(self) -> list[str]:
        """Return a list of volume names accessible via the API server

        Returns:
            A list of volume names returned by the API
        """

        storage_url = self.api_url + "storage/"
        response = requests.get(storage_url, headers=self._get_headers())
        response.raise_for_status()
        return [item["name"] for item in response.json()["items"]]

    def get_subpaths(self, volpath: str) -> list[str]:
        """Return a list of top level directories located under the given volume path

        Args:
            volpath: The volume and path.

        Returns:
            A list of directory names as strings
        """

        storage_url = self.api_url + "storage/" + volpath
        response = requests.get(storage_url, headers=self._get_headers())
        response.raise_for_status()
        return [item["Basename"] for item in response.json()["items"]]

    # Todo: Update docstring and signature after revising the StarfishQuery class
    def submit_query(self, query: str, group_by: str, volpath: str) -> AsyncQuery:
        """Submit a new API query

        Args:
            query: The query to execute
            group_by:
            volpath:

        Returns:
            A ``StarfishQuery`` instance representing the submitted query
        """

        query_url = self.api_url + "async/query/"
        params = {
            "volumes_and_paths": volpath,
            "queries": query,
            "format": "parent_path fn type size blck ct mt at uid gid mode",
            "sort_by": group_by,
            "group_by": group_by,
            "limit": "100000",
            "force_tag_inherit": "false",
            "output_format": "json",
            "delimiter": ",",
            "escape_paths": "false",
            "print_headers": "true",
            "size_unit": "B",
            "humanize_nested": "false",
            "mount_agent": "None",
        }

        req = requests.post(query_url, params=params, headers=self._get_headers())
        response = req.json()
        response.raise_for_status()
        return AsyncQuery(self.api_url, self._get_headers(), response["query_id"])
