import configparser
import optparse
import os
import sys
from concurrent import futures

from crate import client

from inserters import INSERTERS


class DatabaseConfiguration:
    """
    This class reads and holds configuration for database
    """

    host = ""
    port = ""
    user = ""
    password = ""
    timeout_seconds = 5

    def read(self, config_file):
        config = configparser.ConfigParser()

        if config.read(config_file):
            self.host = config["DATABASE"]["host"]
            self.port = config["DATABASE"]["port"]
            self.user = config["DATABASE"]["user"]
            self.password = config["DATABASE"]["password"]
            self.timeout_seconds = int(config["DATABASE"]["timeout_seconds"])
            return True
        return False


class DatabaseClient:
    """
    This class is a main Database client
    """

    _cursor = None

    def init(self, config_file):
        config = DatabaseConfiguration()
        if not config.read(config_file):
            raise Exception("Failed to parse config")

        connection = client.connect("http://{0}:{1}".format(config.host, config.port), username=config.user,
                                    password=config.password, timeout=config.timeout_seconds)

        self._cursor = connection.cursor()

    def get_cursor(self):
        return self._cursor


def main():
    usage = 'usage: main.py [-c config_file -i input_folder]'
    arg_parser = optparse.OptionParser(usage=usage)
    arg_parser.add_option('-c',
                          dest='config_file',
                          type='str',
                          help='configuration file with DB information')
    arg_parser.add_option('-i',
                          dest='input_folder',
                          type='str',
                          help='input folder with information to be inserted')

    (options, args) = (arg_parser.parse_args())

    config_file = vars(options)['config_file']
    input_folder = vars(options)['input_folder']

    tasks = []

    for filename in os.listdir(input_folder):
        fullpath = "{0}/{1}".format(input_folder, filename)

        filename_no_extension = filename.split(".")[0]

        if filename_no_extension not in INSERTERS:
            raise Exception("Failed to parse file: {0}".format(filename))

        db_client = DatabaseClient()
        db_client.init(config_file)

        inserter = INSERTERS[filename_no_extension]()
        tasks.append((inserter, fullpath, db_client.get_cursor()))

    executor = futures.ThreadPoolExecutor(max_workers=4)

    future_results = []
    for (inserter, param1, param2) in tasks:
        future_results.append(executor.submit(inserter.execute, param1, param2))

    for result in future_results:
        result.result()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exception:
        sys.stderr.write('error: {0}'.format(exception))
