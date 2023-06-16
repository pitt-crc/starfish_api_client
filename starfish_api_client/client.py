import asyncio
import logging
import urllib.parse

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
        self._verify = verify

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
        status_response = requests.get(query_status_url, self._headers, verify=self._verify)
        status_response.raise_for_status()
        return status_response.json()["is_done"]

    async def _get_query_result(self) -> dict:
        """Fetch query results from the API

        This method assumes the caller has already checked the query result is
        has been prepared by the API and is ready for consumption.

        Returns:
            The JSON query result
        """

        query_result_url = urllib.parse.urljoin(self._api_url, f'async/query_result/{self.query_id}')

        logging.debug(f'Fetching query result for query {self.query_id} ...')
        response = requests.get(query_result_url, self._headers, verify=self._verify)
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

    def get_subpaths(self, volpath: str) -> list[str]:
        """Return a list of top level directories located under the given volume path

        Args:
            volpath: The volume and path.

        Returns:
            A list of directory names as strings
        """

        storage_url = urllib.parse.urljoin(self.api_url, f'storage/{volpath}')

        logger.info(f'Fetching paths from server under {volpath} ...')
        response = requests.get(storage_url, headers=self._get_headers(), verify=self.verify)
        response.raise_for_status()
        return [item["Basename"] for item in response.json()["items"]]

    def submit_query(
        self,
        query: str,
        volumes_and_paths: str,
        group_by: str,
        format: str = "parent_path fn type size blck ct mt at uid gid mode",
        sort_by: str = None,
        limit: int = 100000,
        force_tag_inherit: bool = False,
        output_format: str = "json",
        delimiter: str = ",",
        escape_paths: bool = False,
        print_headers: bool = True,
        size_unit: str = "B",
        humanize_nested: bool = False,
        mount_agent: str | None = None,
    ) -> AsyncQuery:
        """Submit a new API query

        Returns:
            A ``StarfishQuery`` instance representing the submitted query
        """

        query_url = urllib.parse.urljoin(self.api_url, 'async/query/')
        params = {
            "volumes_and_paths": volumes_and_paths,
            "queries": query,
            "format": format,
            "sort_by": sort_by if sort_by is not None else group_by,
            "group_by": group_by,
            "limit": str(limit),
            "force_tag_inherit": str(force_tag_inherit).lower(),
            "output_format": output_format,
            "delimiter": delimiter,
            "escape_paths": str(escape_paths).lower(),
            "print_headers": str(print_headers).lower(),
            "size_unit": size_unit,
            "humanize_nested": str(humanize_nested).lower(),
            "mount_agent": str(mount_agent),
        }

        logging.info('Submitting new API query ...')
        response = requests.post(query_url, params=params, headers=self._get_headers(), verify=self.verify)
        response.raise_for_status()
        query_id = response.json()["query_id"]

        logging.debug(f'Query returned with id {query_id}')
        return AsyncQuery(self.api_url, self._get_headers(), query_id)
