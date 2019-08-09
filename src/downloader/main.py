#!/usr/bin/env python3

import argparse
import logging
import multiprocessing
import time

from typing import List
from pathlib import Path

from downloader.common import UrlsProvider, OutputStorage
from downloader.aio_downloader import Downloader  # naming is not my best trait
from downloader.phone_parser import PhoneParser

logger = logging.getLogger(__name__)


class ListUrlsProvider(UrlsProvider):
    def __init__(self, urls: List[str]):
        """
        UrlProvider implementation for memory-based list of urls
        :param urls:
        """
        self.urls = urls

    def get_urls(self, max_count=None) -> List[str]:
        result = self.urls if max_count is None else self.urls[:max_count]
        self.urls = self.urls[len(result):]
        return result or None

    @staticmethod
    def from_file(path: Path) -> UrlsProvider:
        with path.open('r') as f:
            urls = list(f.readlines())
            urls = [u.strip() for u in urls if u.strip()]
            return ListUrlsProvider(urls)


class QueueOutputStorage(OutputStorage):
    def __init__(self):
        """
        OutputStorage implementation for multiprocessing queue-based storage
        """
        self.queue = multiprocessing.Queue()

    def put(self, data: dict):
        self.queue.put(data)

    def close(self):
        self.queue.close()


def main(args):
    provider = ListUrlsProvider.from_file(args.urls_path)
    parser = PhoneParser()
    storage = QueueOutputStorage()

    downloaders = []
    for i in range(args.num_processes):
        downloaders.append(Downloader(
            id=i,
            num_downloaders=args.num_processes,
            parser=parser,
            urls_provider=provider,
            storage=storage,
            max_simult_downloads=args.max_downloads_per_process
        ))

    for downloader in downloaders:
        downloader.start()

    stop = False
    while True:
        while not storage.queue.empty():
            # i'm single reader of this queue, so it's safe to call `get` here
            data = storage.queue.get(block=False)
            url = data["url"]
            status = data["status"]
            phones = data.get("phones")
            logging.info(f"got result {url} => {status}: {phones or 'no phones'}")
        if stop:
            break
        if all(not downloader.is_alive() for downloader in downloaders):
            # we can't break here, because it's still possible, that there is some data in the storage.queue
            stop = True
        time.sleep(0.1)


if __name__ == "__main__":
    logging.basicConfig(format='{asctime} {levelname} [{module}] {message}', style='{', level=logging.INFO)
    p = argparse.ArgumentParser()
    p.add_argument("urls_path", type=Path, help="Path to the file with urls, one per line")
    p.add_argument("--num_processes", type=int, default=1, help="Number of processes for downloading/parsing data")
    p.add_argument(
        "--max_downloads_per_process", type=int, default=10, help="Max number of simultaneous downloads per process"
    )
    parsed_args = p.parse_args()
    try:
        main(parsed_args)
    except:
        logger.exception('')
        raise
