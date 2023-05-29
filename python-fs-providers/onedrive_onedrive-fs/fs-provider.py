from dataiku.fsprovider import FSProvider

import os
import shutil

from onedrive_client import OneDriveClient
from onedrive_item import OneDriveItem
from dss_constants import DSSConstants
from io import BytesIO
from safe_logger import SafeLogger

logger = SafeLogger("onedrive plugin", forbiden_keys=["onedrive_credentials"])


class OneDriveFSProvider(FSProvider):
    def __init__(self, root, config, plugin_config):
        """
        :param root: the root path for this provider
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        logger.info("OneDrive fs-provider v1.1.0 init:root={}, config={}, plugin_config={}".format(
            root,
            logger.filter_secrets(config),
            logger.filter_secrets(plugin_config)
        ))
        if len(root) > 0 and root[0] == '/':
            root = root[1:]
        self.root = root

        access_token = config.get('onedrive_connection')['onedrive_credentials']
        self.shared_folder_root = config.get("shared_folder", "").strip("/")
        self.client = OneDriveClient(access_token, shared_folder_root=self.shared_folder_root)

    # util methods
    def get_rel_path(self, path):
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        return path

    def get_lnt_path(self, path):
        if len(path) == 0 or path == '/':
            return '/'
        elts = path.split('/')
        elts = [e for e in elts if len(e) > 0]
        return '/' + '/'.join(elts)

    def get_full_path(self, path):
        ret = "/".join([self.shared_folder_root, self.root.strip("/"), path.strip("/")])
        return ret

    def close(self):
        """
        Perform any necessary cleanup
        """
        logger.info('close')

    def stat(self, path):
        """
        Get the info about the object at the given path inside the provider's root, or None
        if the object doesn't exist
        """
        path = self.get_rel_path(path)
        full_path = self.get_lnt_path(self.get_full_path(path))
        logger.info('stat:path="{}", full_path="{}"'.format(path, full_path))

        onedrive_item = self.client.get_item(full_path)

        if onedrive_item.is_directory():
            return {
                DSSConstants.PATH: self.get_lnt_path(full_path),
                DSSConstants.SIZE: 0,
                DSSConstants.LAST_MODIFIED: onedrive_item.get_last_modified(),
                DSSConstants.IS_DIRECTORY: True
            }
        elif onedrive_item.is_file():
            return {
                DSSConstants.PATH: self.get_lnt_path(full_path),
                DSSConstants.SIZE: onedrive_item.get_size(),
                DSSConstants.LAST_MODIFIED: onedrive_item.get_last_modified(),
                DSSConstants.IS_DIRECTORY: False
            }
        else:
            return None

    def set_last_modified(self, path, last_modified):
        """
        Set the modification time on the object denoted by path. Return False if not possible
        """
        return False

    def browse(self, path):
        """
        List the file or directory at the given path, and its children (if directory)
        """
        path = self.get_rel_path(path)
        full_path = self.get_lnt_path(self.get_full_path(path))
        logger.info('browse:path="{}", full_path="{}"'.format(path, full_path))

        onedrive_item = self.client.get_item(full_path)

        if onedrive_item.is_file():
            return {
                DSSConstants.FULL_PATH: self.get_lnt_path(path),
                DSSConstants.EXISTS: True,
                DSSConstants.DIRECTORY: False,
                DSSConstants.LAST_MODIFIED: onedrive_item.get_last_modified(),
                DSSConstants.SIZE: onedrive_item.get_size()
            }
        elif onedrive_item.is_directory():
            children = []
            for item in self.client.get_children(full_path):
                onedrive_item = OneDriveItem(item)
                sub_path = self.get_lnt_path(os.path.join(path, onedrive_item.get_name()))
                children.append({
                    DSSConstants.FULL_PATH: sub_path,
                    DSSConstants.EXISTS: True,
                    DSSConstants.DIRECTORY: onedrive_item.is_directory(),
                    DSSConstants.LAST_MODIFIED: onedrive_item.get_last_modified(),
                    DSSConstants.SIZE: onedrive_item.get_size()
                })
            return {
                DSSConstants.FULL_PATH: self.get_lnt_path(path),
                DSSConstants.EXISTS: True,
                DSSConstants.DIRECTORY: True,
                DSSConstants.LAST_MODIFIED: onedrive_item.get_last_modified(),
                DSSConstants.CHILDREN: children
            }
        else:
            return {DSSConstants.FULL_PATH: None}

    def enumerate(self, path, first_non_empty):
        """
        Enumerate files recursively from prefix. If first_non_empty, stop at the first non-empty file.

        If the prefix doesn't denote a file or folder, return None
        """
        full_path = self.get_lnt_path(self.get_full_path(path))
        logger.info('enumerate:path="{}", full_path="{}"'.format(path, full_path))

        onedrive_item = self.client.get_item(full_path)

        if not onedrive_item.exists():
            return None

        if onedrive_item.is_file():
            return [{
                DSSConstants.PATH: path,
                DSSConstants.SIZE: onedrive_item.get_size(),
                DSSConstants.LAST_MODIFIED: onedrive_item.get_last_modified()
            }]
        return self.list_recursive(path, full_path, first_non_empty)

    def list_recursive(self, path, full_path, first_non_empty):
        paths = []
        for child in self.client.get_children(full_path):
            onedrive_child = OneDriveItem(child)
            if onedrive_child.is_directory():
                paths.extend(self.list_recursive(
                    self.get_lnt_path(path + "/" + onedrive_child.get_name()),
                    self.get_lnt_path(full_path + "/" + onedrive_child.get_name()),
                    first_non_empty
                ))
            else:
                paths.append({
                    DSSConstants.PATH: self.get_lnt_path(path + "/" + onedrive_child.get_name()),
                    DSSConstants.SIZE: onedrive_child.get_size()
                })
                if first_non_empty:
                    return paths
        return paths

    def delete_recursive(self, path):
        """
        Delete recursively from path. Return the number of deleted files (optional)
        """
        full_path = self.get_full_path(path)
        logger.info('delete_recursive:path="{}", full_path="{}"'.format(path, full_path))
        self.assert_path_is_valid(full_path)
        response = self.client.delete(full_path)
        if response.status_code == 204:
            return 1

    def move(self, from_path, to_path):
        """
        Move a file or folder to a new path inside the provider's root. Return false if the moved file didn't exist
        """
        full_from_path = self.get_full_path(from_path)
        full_to_path = self.get_full_path(to_path)
        logger.info('move:from "{}", to "{}"'.format(full_from_path, full_to_path))

        path, from_filename = os.path.split(full_from_path)
        path, to_filename = os.path.split(full_to_path)
        if from_filename == to_filename:
            return self.client.move(full_from_path, full_to_path)
        else:
            return self.client.rename(full_from_path, full_to_path)

    def read(self, path, stream, limit):
        """
        Read the object denoted by path into the stream. Limit is an optional bound on the number of bytes to send
        """
        full_path = self.get_full_path(path)
        logger.info('read:path="{}", full_path="{}"'.format(path, full_path))

        response = self.client.get_content(full_path)
        if response.status_code == 404:
            logger.error("File not found")
            return
        bio = BytesIO(response.content)
        shutil.copyfileobj(bio, stream)

    def write(self, path, stream):
        """
        Write the stream to the object denoted by path into the stream
        """
        full_path = self.get_full_path(path)
        logger.info('write:path="{}", full_path="{}"'.format(path, full_path))

        bio = BytesIO()
        shutil.copyfileobj(stream, bio)
        bio.seek(0)
        self.client.upload(full_path, bio)

    def assert_path_is_valid(self, path):
        if path is None:
            raise Exception("Cannot delete root path")
        path = self.get_rel_path(path)
        if path == "" or path == "/":
            raise Exception("Cannot delete root path")
