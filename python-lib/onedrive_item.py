import json

from datetime import datetime
from onedrive_constants import *

class OneDriveItem():

    def __init__(self, description):
        self.description = description
        self._exists = ("@odata.context" in self.description)

    def is_directory(self):
        return ONEDRIVE_FOLDER in self.description

    def is_file(self):
        return ONEDRIVE_FILE in self.description

    def get_size(self):
        if ONEDRIVE_SIZE in self.description:
            return self.description[ONEDRIVE_SIZE]
        else:
            return None

    def get_id(self):
        if ONEDRIVE_ID in self.description:
            return self.description[ONEDRIVE_ID]
        else:
            return None

    def get_name(self):
        if ONEDRIVE_NAME in self.description:
            return self.description[ONEDRIVE_NAME]
        else:
            return None

    def get_last_modified(self):
        if ONEDRIVE_LAST_MODIFIED in self.description:
            return self.format_date(self.description[ONEDRIVE_LAST_MODIFIED])
        else:
            return None

    def format_date(self, date):
        if date is not None:
            utc_time = datetime.strptime(date, ONEDRIVE_TIME_FORMAT)
            epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
            return int(epoch_time) * 1000
        else:
            return None

    def exists(self):
        return self._exists