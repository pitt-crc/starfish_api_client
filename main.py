import logging
import time

import requests

logger = logging.getLogger('starfish_client')


class StarFishServer:
    """Class for interacting with a StarFish API server."""

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
        """Authenticate against the StarFish API

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
        """ Generate a list of the volumes available on the server.

        Returns:
            A list of volume names returned by the API
        """

        storage_url = self.api_url + "storage/"
        response = requests.get(storage_url, headers=self._get_headers())
        response.raise_for_status()
        return [item["name"] for item in response.json()["items"]]

    def get_subpaths(self, volpath):
        """Generate list of directories in top layer of designated volpath.
        Parameters
        ----------
        volpath : string
            The volume and path.
        Returns
        -------
        subpaths : list of strings
        """
        getsubpaths_url = self.api_url + "storage/" + volpath
        request = return_get_json(getsubpaths_url, self._get_headers())
        pathdicts = request["items"]
        subpaths = [i["Basename"] for i in pathdicts]
        return subpaths

    def create_query(self, query, group_by, volpath, sec=3):
        """Produce a Query class object.
        Parameters
        ----------
        query : string
        group_by : string
        volpath : string
        sec : integer, optional

        Returns
        -------
        query : Query class object
        """
        query = StarFishQuery(
            self._get_headers(), self.api_url, query, group_by, volpath, sec=sec
        )
        return query

    def get_vol_membership(self, volume, mtype):
        """Get the membership of the provided volume.
        """
        url = self.api_url + f"mapping/{mtype}_membership?volume_name=" + volume
        member_list = return_get_json(url, self._get_headers())
        return member_list


class StarFishQuery:
    """

    Attributes
    ----------
    api_url : str
    headers : dict
    query_id : str
    result : list

    Methods
    -------
    post_async_query(query, group_by, volpath)
    return_results_once_prepared(sec=3)
    return_query_result()
    """

    def __init__(self, headers, api_url, query, group_by, volpath, sec=3):
        self.api_url = api_url
        self.headers = headers
        self.query_id = self.post_async_query(query, group_by, volpath)
        self.result = self.return_results_once_prepared(sec=sec)

    def post_async_query(self, query, group_by, volpath):
        """Post an asynchronous query through the Starfish API.
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
        req = requests.post(query_url, params=params, headers=self.headers)
        response = req.json()
        logger.debug("response: %s", response)
        return response["query_id"]

    def return_results_once_prepared(self, sec=3):
        """Wait for posted query to return result.
        """
        while True:
            query_check_url = self.api_url + "async/query/" + self.query_id
            response = return_get_json(query_check_url, self.headers)
            if response["is_done"] == True:
                result = self.return_query_result()
                return result
            time.sleep(sec)

    def return_query_result(self):
        """Go to link for query result and return the JSON.
        """
        query_result_url = self.api_url + "async/query_result/" + self.query_id
        response = return_get_json(query_result_url, self.headers)
        return response


def return_get_json(url, headers):
    """return JSON from the designated url using the designated headers.
    """
    response = requests.get(url, headers=headers)
    return response.json()
