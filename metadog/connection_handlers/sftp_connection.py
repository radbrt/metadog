import fsspec

class SFTPFileSystem():
    def __init__(self, host, username, password, port=22, search_prefix='./'):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.search_prefix = search_prefix

        self.connection = fsspec.filesystem('sftp', host=self.host, username=self.username, password=self.password)


    def get_files(self):
        fl = self.connection.ls(self.search_prefix)

        return fl


    def get_file(self, file_name):
        file_stream = self.connection.open(file_name, 'rb')

        return file_stream
    
    