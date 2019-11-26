import json

from datetime import datetime

class OneDriveItem():

    def __init__(self, description):
        self.description = description
        self._exists = ("@odata.context" in self.description)

    def is_directory(self):
        return "folder" in self.description

    def is_file(self):
        return "file" in self.description

    def get_size(self):
        if "size" in self.description:
            return self.description["size"]
        else:
            return None

    def get_id(self):
        if "id" in self.description:
            return self.description["id"]
        else:
            return None

    def get_name(self):
        if "name" in self.description:
            return self.description["name"]
        else:
            return None

    def get_last_modified(self):
        if "lastModifiedDateTime" in self.description:
            return self.format_date(self.description["lastModifiedDateTime"])
        else:
            return None

    def format_date(self, date):
        if date is not None:
            utc_time = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
            epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
            return int(epoch_time) * 1000
        else:
            return None

    def exists(self):
        return self._exists