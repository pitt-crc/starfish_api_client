import json
import logging
import time
from datetime import datetime

import requests

datestr = datetime.today().strftime("%Y%m%d")
logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.DEBUG)
filehandler = logging.FileHandler(f'coldfront/plugins/sftocf/data/logs/sfc{datestr}.log', 'w')
logger.addHandler(filehandler)

with open("coldfront/plugins/sftocf/servers.json", "r") as myfile:
    svp = json.loads(myfile.read())


class StarFishServer:
    """Class for interacting with StarFish API.
    """

    def __init__(self, server, url):
        self.name = server
        self.api_url = f"{url}/api/"
        self.token = self.get_auth_token()
        self.headers = generate_headers(self.token)
        self.volumes = self.get_volume_names()

    def get_auth_token(self):
        """Obtain a token through the auth endpoint.
        """
        username = import_from_settings('SFUSER')
        password = import_from_settings('SFPASS')
        auth_url = self.api_url + "auth/"
        todo = {"username": username, "password": password}
        response = requests.post(auth_url, json=todo)
        # response.status_code
        response_json = response.json()
        token = response_json["token"]
        return token

    # 2A. Generate list of volumes to search, along with top-level paths

    def get_volume_names(self):
        """ Generate a list of the volumes available on the server.
        """
        stor_url = self.api_url + "storage/"
        response = return_get_json(stor_url, self.headers)
        volnames = [i["name"] for i in response["items"]]
        return volnames

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
        request = return_get_json(getsubpaths_url, self.headers)
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
            self.headers, self.api_url, query, group_by, volpath, sec=sec
        )
        return query

    def get_vol_membership(self, volume, mtype):
        """Get the membership of the provided volume.
        """
        url = self.api_url + f"mapping/{mtype}_membership?volume_name=" + volume
        member_list = return_get_json(url, self.headers)
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


def generate_headers(token):
    """Generate "headers" attribute by using the "token" attribute.
    """
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer {}".format(token),
    }
    return headers
