import os, requests, shutil

try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

from onedrive_item import OneDriveItem

class OneDriveClient():
    access_token = None
    CHUNK_SIZE = 320 * 1024
    DRIVE_API_URL = "https://api.onedrive.com/v1.0/drive/"
    ITEMS_API_URL = "https://api.onedrive.com/v1.0/drive/items/"

    def __init__(self, access_token):
        self.access_token = access_token

    def upload(self, path, file_handle):
        # https://docs.microsoft.com/fr-fr/onedrive/developer/rest-api/api/driveitem_createuploadsession?view=odsp-graph-online
        upload_url = self.create_upload_session(path, metadata = None)
        self.upload_loop(file_handle, upload_url)

    def upload_loop(self, file_handle, url):
        file_size = self.file_size(file_handle)
        file_handle.seek(0)
        next_expected_range_low = 0
        
        while file_handle.tell() < file_size:
            response = self.put(file_handle.read(self.CHUNK_SIZE), url, next_expected_range_low, file_size)
            next_expected_range_low = next_expected_range_low + self.CHUNK_SIZE

    def put(self, data, url, next_expected_range_low, file_size):
        headers = {
            "authorization": 'bearer ' + self.access_token,
            "Content-Length": "{}".format(len(data)),
            "Content-Range": "bytes {}-{}/{}".format(next_expected_range_low, next_expected_range_low + len(data) - 1, file_size)
        }
        response = requests.put(url, headers=headers, data=data)
        return response

    def file_size(self, file_handle):
        file_handle.seek(0, 2)
        return file_handle.tell()

    def create_upload_session(self, path, metadata=None):
        response = self.post(path, command = "createUploadSession")
        response_json = response.json()
        if "uploadUrl" in response_json:
            return response_json["uploadUrl"]
        else:
            raise Exception("Can't create upload session")

    def loop_items(self, response):
        if "value" in response:
            return response["value"]
        else:
            return None

    def onedrive_path(self, path):
        if path == "" or path == "/":
            return "root"
        else:
            return "root:" + path + ":"

    def create_directory(self, path):
        return

    def post(self, path, command=None, metadata=None):
        onedrive_path = self.onedrive_path(path)
        if command is None:
            command = ""
        else:
            command = "/" + command
        response = requests.post( self.ITEMS_API_URL + onedrive_path + command, headers=self.generate_header())
        return response

    def get_upload_metadata(self, name, description=None):
        metadata = {
            "item":{
                "@microsoft.graph.conflictBehavior": "rename",
                "name": name
            }
        }
        if description is not None:
            metadata["item"]["description"] = description
        return metadata

    def move(self, from_path, to_path):
        from_item = self.get(from_path)
        path, filename = os.path.split(from_path)
        target_path, target_filename = os.path.split(to_path)
        if not from_item.exists():
            return False
        to_item = self.get(target_path)
        onedrive_from_path = self.onedrive_path(from_path)
        response = requests.patch( self.ITEMS_API_URL + onedrive_from_path,
            headers = self.generate_header(content_type="application/json"),
            json = self.generate_move_header(filename, to_item.get_id())
        )
        return True

    def rename(self, from_path, to_path):
        path, filename = os.path.split(to_path)
        onedrive_from_path = self.onedrive_path(from_path)
        response = requests.patch(
            self.ITEMS_API_URL + onedrive_from_path,
            headers = self.generate_header(content_type="application/json"),
            json = self.generate_rename_header(filename)
        )
        return True

    def get(self, path):
        onedrive_path = self.onedrive_path(path)
        response = requests.get( self.DRIVE_API_URL + onedrive_path, headers=self.generate_header())
        onedrive_item = OneDriveItem(response.json())
        return onedrive_item

    def delete(self, path):
        onedrive_path = self.onedrive_path(path)
        response = requests.delete( self.ITEMS_API_URL + onedrive_path, headers=self.generate_header())
        return response

    def get_children(self, path):
        request = self.onedrive_path(path)
        response = requests.get( self.DRIVE_API_URL + request + "/children", headers=self.generate_header())
        return response

    def get_content(self, path):
        request = self.onedrive_path(path)
        response = requests.get( self.DRIVE_API_URL + request + "/content", headers=self.generate_header())
        return response

    def generate_header(self, content_type=None):
        header = {
            'content-Type': 'application/x-www-form-urlencoded',
            'authorization': 'bearer ' + self.access_token
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