from typing import List, Tuple, Union


class UrlsProvider(object):
    def get_urls(self, max_count=None) -> List[str]:
        """
        Returns portion of urls (at most `max_count` if specified).
        When there are no more urls, returns None.
        It's possible that it would return empty list.
        It means that there are no more urls right now, but still possible that they'll arrive in the future
        :param max_count:
        """
        raise NotImplementedError()


class ParserBase(object):
    def process_content(self, content: Union[bytes, str]) -> Tuple[str, dict]:
        raise NotImplementedError()


class OutputStorage(object):
    def put(self, data: dict):
        raise NotImplementedError()

    def close(self):
        pass
