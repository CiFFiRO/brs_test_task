import os
import zipfile
import configparser
import dataclasses
import datetime
import collections
import queue
import time
import pathlib
import logging
import typing


@dataclasses.dataclass
class File:
    size: int
    path: str
    born_date: datetime.date


@dataclasses.dataclass(order=True)
class PrioritizedItem:
    priority: int
    item: typing.Any = dataclasses.field(compare=False)


class Storage:
    def __init__(self):
        self._queue_remove = queue.PriorityQueue()
        self._exist_files = set()
        self._sum_size = 0

    def is_file_exist(self, file_name):
        return file_name in self._exist_files

    def add_file(self, file):
        item = PrioritizedItem(priority=(file.born_date - datetime.date(1970, 1, 1)).days, item=file)
        self._queue_remove.put_nowait(item)
        self._exist_files.add(file.path)
        self._sum_size += file.size

    def age_oldest_file(self):
        if self._queue_remove.empty():
            return None
        return self._queue_remove.queue[0].priority

    def pop_oldest_file(self):
        if self._queue_remove.empty():
            return None
        file = self._queue_remove.get_nowait().item
        self._exist_files.remove(file.path)
        self._sum_size -= file.size
        return file

    def is_empty(self):
        return self._queue_remove.empty()

    def size_collection(self):
        return self._sum_size


def update(storage, config):
    queue = collections.deque()
    queue.appendleft(config['Condition']['STORAGE_DIRECTORY'])
    while len(queue) > 0:
        file_name = queue.pop()
        if os.path.isdir(file_name):
            for name in os.listdir(file_name):
                if name.startswith('.'):
                    continue
                name = os.path.join(file_name, name)
                queue.appendleft(name)
        elif not storage.is_file_exist(file_name):
            path = os.path.normpath(file_name).split(os.sep)
            file = File(size=os.path.getsize(file_name), path=file_name,
                        born_date=datetime.date(int(path[-4]), int(path[-3]), int(path[-2])))
            storage.add_file(file)
            logging.info(f'Detect new file: {file.path}')


def clear(storage, config):
    def to_str(number):
        return f'0{number}' if number < 10 else str(number)

    now = (datetime.date.today()-datetime.date(1970, 1, 1)).days
    while not storage.is_empty() and \
            now - storage.age_oldest_file() >= int(config['Condition']['THRESHOLD_FILES_OLD_DAYS']) and \
            int(config['Condition']['STORAGE_ALL_SPACE'])*(1.0 - float(config['Condition']['THRESHOLD_FREE_SPACE'])) - storage.size_collection() < 0:
        file = storage.pop_oldest_file()
        dst_folder = os.path.join(config['Condition']['ARCHIVE_DIRECTORY'], to_str(file.born_date.year),
                                  to_str(file.born_date.month), to_str(file.born_date.day))
        pathlib.Path(dst_folder).mkdir(parents=True, exist_ok=True)
        file_name = os.path.normpath(file.path).split(os.sep)[-1]
        dst_file = os.path.join(dst_folder, file_name+'.zip')
        with zipfile.ZipFile(dst_file, 'w') as arch_file:
            arch_file.write(file.path, file_name)
        try:
            logging.info(f'Move to archive file {file.path}')
            os.remove(file.path)
        except:
            logging.info(f'ERROR: Can\'t delete file {file.path}')


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('settings.ini')
    storage = Storage()
    logging.basicConfig(filename=config['Condition']['LOG_FILE'], level=logging.INFO)

    while True:
        update(storage, config)
        clear(storage, config)
        time.sleep(int(config['Condition']['CHECK_TIME_DELAY']))
