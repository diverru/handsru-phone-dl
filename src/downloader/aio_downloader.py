import asyncio
import binascii
import logging
import multiprocessing

from aiohttp import ClientSession, TCPConnector
from aiohttp.client_exceptions import InvalidURL, ServerDisconnectedError, ServerTimeoutError,\
    ClientConnectorError, ClientHttpProxyError, ClientOSError
from concurrent.futures import TimeoutError

from downloader.common import UrlsProvider, ParserBase, OutputStorage


logger = logging.getLogger(__name__)


def get_unsigned_crc32(s: str) -> int:
    """
    Return unsigned CRC32 of the given string
    :param s:
    :return:
    """
    return binascii.crc32(s.encode("utf8")) & 0xFFFFFFFF


class Downloader(object):
    # just to show concurrency for small number of urls
    BATCH_SIZE = 1
    # wait interval before polling UrlsProvider (when it has returned empty list), seconds
    BATCH_WAIT_DURATION = 10

    def __init__(
        self,
        id: int,
        num_downloaders: int,
        urls_provider: UrlsProvider,
        parser: ParserBase,
        storage: OutputStorage,
        max_simult_downloads: int
    ):
        """
        Class for downloading and processing urls
        :param id: identifier of this downloader, should be in range 0..num_downloaders - 1
        :param num_downloaders: total number of downloaders (across all processes)
        :param urls_provider: source of urls
        :param parser: downloaded content parser
        :param storage: storage download & parse results
        :param max_simult_downloads: maximum number of simultaneous downloads per Downloader instance
        """
        assert 0 <= id < num_downloaders
        self.id = id
        self.num_downloaders = num_downloaders
        self.urls_provider = urls_provider
        self.max_simult_downloads = max_simult_downloads
        self.parser = parser
        self.storage = storage

        self._process = multiprocessing.Process(target=self._run)

    def start(self):
        """
        Start this downloader
        """
        self._process.start()

    async def _process_batch(self, connector: TCPConnector, urls: list):
        """
        Process all urls (that are in concern of this Downloader)
        :param connector:
        :param urls:
        """
        urls = [u for u in urls if get_unsigned_crc32(u) % self.num_downloaders == self.id]
        for url in urls:
            await self._download_single_url(url, connector)

    def _write_single_result(self, url: str, status: str, data: dict):
        """
        Put result into the storage
        :param url:
        :param status:
        :param data:
        """
        self.storage.put({
            "url": url,
            "status": status,
            **data
        })

    async def _download_single_url(self, url: str, connector: TCPConnector):
        """
        Download single url
        :param url:
        :param connector:
        :return:
        """
        try:
            async with ClientSession(connector=connector, connector_owner=False) as session:
                logger.info(f"downloading {url}")
                async with session.get(url, verify_ssl=False) as res:
                    if not (200 <= res.status < 300):
                        self._write_single_result(url, f"ERROR:HTTP{res.status}", {})
                        return
                    content = await res.read()
                    status, data = self.parser.process_content(content)
                    self._write_single_result(url, status, data)
        except (TimeoutError, InvalidURL, ServerDisconnectedError, ServerTimeoutError, ClientConnectorError,
                ClientHttpProxyError, ClientOSError) as e:
            self._write_single_result(url, "ERROR:NETWORK", {})
            return
        except Exception:
            self._write_single_result(url, "ERROR:UNKNOWN", {})
            raise

    async def _batch_generator(self):
        """
        Generates batches from UrlProvider
        """
        while True:
            urls = self.urls_provider.get_urls(self.BATCH_SIZE)
            if urls is None:
                break
            if not urls:
                await asyncio.sleep(self.BATCH_WAIT_DURATION)
                continue
            yield urls

    async def _download_all(self):
        """
        Download everything from UrlProvider
        """
        connector = TCPConnector(
            verify_ssl=False,
            limit=self.max_simult_downloads,
            limit_per_host=2  # let's be polite
        )
        async with connector:
            batch_gen = self._batch_generator()

            async def worker():
                async for batch in batch_gen:
                    await self._process_batch(connector, batch)
            # split tasks by workers to run them concurrently
            # this allows to use generator for source urls
            await asyncio.gather(*[worker() for _ in range(self.max_simult_downloads)])

    def _run(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self._download_all())
        loop.run_until_complete(future)
        # flush any buffered data
        self.storage.close()

    def join(self, timeout=None):
        """
        Wait until internal process finishes
        :param timeout:
        """
        self._process.join(timeout)

    def is_alive(self) -> bool:
        """
        Check whether internal process alive or not
        :return:
        """
        return self._process.is_alive()
