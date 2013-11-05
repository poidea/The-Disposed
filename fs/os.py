from fs import driver
import mimetypes
import shutil

def get_mimetype(filename):
    type, encoding = mimetypes.guess_type(filename)
    return type

class FileSystem(driver.FileSystemDriver):
    def read(self, path):
        f = open(self.locate(path), 'rb')
        content = f.read()
        f.close()
        return content

    def write(self, path, content):
        f = open(self.locate(path), 'wb')
        f.write(content)
        f.close()

    def locate(self, path):
        return '/home/pin/static' + path

    def mimetype(self, path):
        return get_mimetype(self.locate(path))

    def copy(self, src, dest):
        shutil.copy2(self.locate(src), self.locate(dest))
