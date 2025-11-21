import os
import requests
from time import sleep

from onedrive_item import OneDriveItem
from onedrive_constants import OneDriveConstants
from safe_logger import SafeLogger
from common import get_value_from_path

logger = SafeLogger("onedrive plugin", forbiden_keys=["onedrive_credentials"])


class OneDriveClient():
    access_token = None
    CHUNK_SIZE = 320 * 1024
    DRIVE_API_URL = "https://graph.microsoft.com/v1.0/me/drive/"
    ITEMS_API_URL = "https://graph.microsoft.com/v1.0/me/drive/items/"
    SHARED_API_URL = "https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}:"
    SHARED_WITH_ME_URL = "https://graph.microsoft.com/v1.0/me/drive/sharedWithMe"

    def __init__(self, access_token, shared_folder_root=""):
        self.access_token = access_token
        self.shared_with_me = None
        self.drive_id = None
        self.shared_folder_root = shared_folder_root
        self.session = requests.Session()
        self.session.auth = BearerTokenAuth(access_token)
        if shared_folder_root:
            shared_folder_root = shared_folder_root.strip("/")
            self.drive_id = self.get_shared_directory_drive_id(shared_folder_root)

    def upload(self, path, file_handle):
        # https://docs.microsoft.com/fr-fr/onedrive/developer/rest-api/api/driveitem_createuploadsession?view=odsp-graph-online
        upload_url = self.create_upload_session(path)
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
            "Content-Length": "{}".format(len(data)),
            "Content-Range": "bytes {}-{}/{}".format(next_expected_range_low, next_expected_range_low + len(data) - 1, file_size)
        }
        response = self.session.put(url, headers=headers, data=data)
        return response

    def file_size(self, file_handle):
        file_handle.seek(0, 2)
        return file_handle.tell()

    def create_upload_session(self, path):
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
        headers = self.generate_header()
        response = self.session.post(self.get_path_endpoint(path, is_item=True) + command, headers=headers)
        return response

    def move(self, from_path, to_path):
        from_item = self.get_item(from_path)
        path, filename = os.path.split(from_path)
        target_path, target_filename = os.path.split(to_path)
        if not from_item.exists():
            return False
        to_item = self.get_item(target_path)
        self.session.patch(
            self.get_path_endpoint(from_path, is_item=True),
            headers=self.generate_header(content_type="application/json"),
            json=self.generate_move_header(filename, to_item.get_id())
        )
        return True

    def rename(self, from_path, to_path):
        path, filename = os.path.split(to_path)
        self.session.patch(
            self.get_path_endpoint(from_path, is_item=True),
            headers=self.generate_header(content_type="application/json"),
            json=self.generate_rename_header(filename)
        )
        return True

    def get_item(self, path):
        headers = self.generate_header()
        endpoint = self.get_path_endpoint(path)
        response = self.session.get(endpoint, headers=headers)
        onedrive_item = OneDriveItem(response.json())
        return onedrive_item

    def get_shared_directory_drive_id(self, shared_directory_name):
        url = "https://graph.microsoft.com/v1.0/me/drive/root:/{}:".format(shared_directory_name)
        item = self.get(url, headers=self.generate_header())
        drive_id = get_value_from_path(item, ["remoteItem", "parentReference", "driveId"])
        return drive_id

    def delete(self, path):
        response = self.session.delete(self.get_path_endpoint(path, is_item=True), headers=self.generate_header())
        return response

    def get_children(self, path):
        url = self.get_path_endpoint(path) + "/children"
        while url:
            response = self.session.get(url, headers=self.generate_header())
            assert_response_ok(response)
            json_response = response.json()
            next_page_url = get_next_page_url(json_response)
            url = assert_no_loop_condition(url, next_page_url)
            children = json_response.get(OneDriveConstants.VALUE_CONTAINER, [])
            for child in children:
                yield child

    def get_content(self, path):
        response = self.session.get(self.get_path_endpoint(path) + "/content", headers=self.generate_header())
        return response

    def get_path_endpoint(self, path, drive=None, is_item=False):
        onedrive_path = self.onedrive_path(path)
        if self.drive_id:
            return self.SHARED_API_URL.format(drive_id=self.drive_id, file_path=path.strip('/'))
        else:
            endpoint_root = self.ITEMS_API_URL if is_item else self.DRIVE_API_URL
        return endpoint_root + onedrive_path

    def get(self, url, headers=None):
        headers = headers or {}
        headers.update({'Content-Type': 'application/json'})
        response = self.session.get(url, headers=headers)
        assert_response_ok(response)
        json_response = response.json()
        return json_response

    def generate_header(self, content_type=None):
        header = {
            'Content-Type': 'application/json'
        }
        if content_type is not None:
            header['Content-Type'] = content_type
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


def assert_response_ok(response, context=None, can_raise=True):
    error_message = None
    response_has_content = False
    if type(response) is not requests.models.Response:
        error_message = "Incorrect response"
    else:
        response_has_content = True
        status_code = response.status_code
        if status_code < 400:
            return None
        error_message = "Error {}".format(status_code)
    if context:
        error_message += " while {}".format(context)
    if can_raise:
        if response_has_content:
            logger.error("Dumping response content:{}".format(response.content))
        raise Exception(error_message)
    return error_message


def get_next_page_url(json_response):
    next_page_url = json_response.get(OneDriveConstants.NEXT_URL_KEY)
    if next_page_url:
        logger.info("Next page found:{}".format(next_page_url))
    return next_page_url


def assert_no_loop_condition(current_url, next_url):
    if current_url == next_url:
        error_message = "Loop condition on url {}".format(next_url)
        logger.error(error_message)
        raise Exception(error_message)
    return next_url


class BearerTokenAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, response):
        response.headers["authorization"] = "Bearer {}".format(self.token)
        return response
