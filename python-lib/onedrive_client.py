import os
import requests
from time import sleep

from onedrive_item import OneDriveItem
from onedrive_constants import OneDriveConstants
from safe_logger import SafeLogger

logger = SafeLogger("onedrive plugin", forbiden_keys=["onedrive_credentials"])


class OneDriveClient():
    access_token = None
    CHUNK_SIZE = 320 * 1024
    DRIVE_API_URL = "https://graph.microsoft.com/v1.0/me/drive/"
    ITEMS_API_URL = "https://graph.microsoft.com/v1.0/me/drive/items/"

    def __init__(self, access_token):
        self.access_token = access_token

    def upload(self, path, file_handle):
        # https://docs.microsoft.com/fr-fr/onedrive/developer/rest-api/api/driveitem_createuploadsession?view=odsp-graph-online
        upload_url = self.create_upload_session(path, metadata=None)
        self.upload_loop(file_handle, upload_url)

    def upload_loop(self, file_handle, url):
        file_size = self.file_size(file_handle)
        file_handle.seek(0)
        next_expected_range_low = 0

        while file_handle.tell() < file_size:
            self.put(file_handle.read(self.CHUNK_SIZE), url, next_expected_range_low, file_size)
            next_expected_range_low = next_expected_range_low + self.CHUNK_SIZE

    def put(self, data, url, next_expected_range_low, file_size):
        headers = {
            "Authorization": 'bearer {}'.format(self.access_token),
            "Content-Length": "{}".format(len(data)),
            "Content-Range": "bytes {}-{}/{}".format(next_expected_range_low, next_expected_range_low + len(data) - 1, file_size)
        }
        response = requests.put(url, headers=headers, data=data)
        return response

    def file_size(self, file_handle):
        file_handle.seek(0, 2)
        return file_handle.tell()

    def create_upload_session(self, path, metadata=None):
        number_retries = OneDriveConstants.NB_RETRIES_ON_CREATE_UPLOAD_SESSION
        while number_retries:
            logger.info("create_upload_session post to {}".format(path))
            response = self.post(path, command=OneDriveConstants.CREATE_UPLOAD_SESSION)
            response_json = response.json()
            if OneDriveConstants.UPLOAD_URL in response_json:
                return response_json[OneDriveConstants.UPLOAD_URL]
            else:
                # When preceded by a delete, create_upload_session can return an itemNotFound error
                # We wait a second before retrying
                if get_value_from_path(response_json, ["error", "code"]) == "itemNotFound" and number_retries:
                    number_retries -= 1
                    logger.info("itemNotFound error on create_upload_session, retrying")
                    sleep(OneDriveConstants.TIME_BEFORE_RETRIES)
                else:
                    raise Exception("Can't create upload session")

    def loop_items(self, response):
        if OneDriveConstants.VALUE_CONTAINER in response:
            return response[OneDriveConstants.VALUE_CONTAINER]
        else:
            return None

    def onedrive_path(self, path):
        if path == "" or path == "/":
            return OneDriveConstants.ROOT
        else:
            return "root:" + path + ":"

    def create_directory(self, path):
        return

    def post(self, path, command=None, metadata=None):
        if command is None:
            command = ""
        else:
            command = "/" + command
        response = requests.post(self.get_path_endpoint(path, is_item=True) + command, headers=self.generate_header())
        return response

    def get_upload_metadata(self, name, description=None):
        metadata = {
            "item": {
                "@microsoft.graph.conflictBehavior": "rename",
                "name": name
            }
        }
        if description is not None:
            metadata[OneDriveConstants.ITEM][OneDriveConstants.DESCRIPTION] = description
        return metadata

    def move(self, from_path, to_path):
        from_item = self.get_item(from_path)
        path, filename = os.path.split(from_path)
        target_path, target_filename = os.path.split(to_path)
        if not from_item.exists():
            return False
        to_item = self.get_item(target_path)
        requests.patch(
            self.get_path_endpoint(from_path, is_item=True),
            headers=self.generate_header(content_type="application/json"),
            json=self.generate_move_header(filename, to_item.get_id())
        )
        return True

    def rename(self, from_path, to_path):
        path, filename = os.path.split(to_path)
        requests.patch(
            self.get_path_endpoint(from_path, is_item=True),
            headers=self.generate_header(content_type="application/json"),
            json=self.generate_rename_header(filename)
        )
        return True

    def get_item(self, path):
        headers = self.generate_header()
        response = requests.get(self.get_path_endpoint(path), headers=headers)
        onedrive_item = OneDriveItem(response.json())
        return onedrive_item

    def delete(self, path):
        response = requests.delete(self.get_path_endpoint(path, is_item=True), headers=self.generate_header())
        return response

    def get_children(self, path):
        response = requests.get(self.get_path_endpoint(path) + "/children", headers=self.generate_header())
        return response

    def get_content(self, path):
        response = requests.get(self.get_path_endpoint(path) + "/content", headers=self.generate_header())
        return response

    def get_path_endpoint(self, path, drive=None, is_item=False):
        onedrive_path = self.onedrive_path(path)
        endpoint_root = self.ITEMS_API_URL if is_item else self.DRIVE_API_URL
        return endpoint_root + onedrive_path

    def generate_header(self, content_type=None):
        header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'bearer {}'.format(self.access_token)
            }
        if content_type is not None:
            header['content-Type'] = content_type
        return header

    def generate_move_header(self, name, parent_reference_id):
        header = {
            'parentReference': {
                    'id': parent_reference_id
                },
            'name': name
        }
        return header

    def generate_rename_header(self, name):
        header = {
            'name': name
        }
        return header


def get_value_from_path(dictionary, path, default_reply=None):
    ret = dictionary
    for key in path:
        if key in ret:
            ret = ret.get(key)
        else:
            return default_reply
    return ret
