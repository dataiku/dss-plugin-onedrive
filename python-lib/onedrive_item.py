from datetime import datetime
from onedrive_constants import OneDriveConstants


class OneDriveItem():

    def __init__(self, description):
        self.description = description or {}
        self._exists = ("@odata.context" in self.description)

    def is_directory(self):
        return OneDriveConstants.FOLDER in self.description

    def is_file(self):
        return OneDriveConstants.FILE in self.description

    def get_size(self):
        if OneDriveConstants.SIZE in self.description:
            return self.description[OneDriveConstants.SIZE]
        else:
            return None

    def get_id(self):
        if OneDriveConstants.ID in self.description:
            return self.description[OneDriveConstants.ID]
        else:
            return None

    def get_name(self):
        if OneDriveConstants.NAME in self.description:
            return self.description[OneDriveConstants.NAME]
        else:
            return None

    def get_last_modified(self):
        if OneDriveConstants.LAST_MODIFIED in self.description:
            return self.format_date(self.description[OneDriveConstants.LAST_MODIFIED])
        else:
            return None

    def format_date(self, date):
        if date is not None:
            try:
                utc_time = datetime.strptime(date, OneDriveConstants.TIME_FORMAT)
            except Exception:
                # freak incident when ms are = 0, can be ignored
                return None
            epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
            return int(epoch_time) * 1000
        else:
            return None

    def exists(self):
        return self._exists
