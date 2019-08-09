import re

from bs4 import BeautifulSoup
from phonenumbers import PhoneNumberMatcher, PhoneNumberFormat, Leniency
from phonenumbers import format_number
from phonenumbers.phonenumberutil import is_valid_number
from typing import Tuple, Union

from downloader.common import ParserBase


class PhoneParser(ParserBase):
    RE_EXTRA_CHARS = re.compile(r"[()-]|\s")
    DEFAULT_CODE = "495"

    def process_content(self, content: Union[bytes, str]) -> Tuple[str, dict]:
        """
        Process given source of html page and extract all phones
        :param content:
        :return: tuple with status and dict {"phones": 'csv-list of phone numbers in format 80123456789'}
        """
        soup = BeautifulSoup(content, 'html.parser')

        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.decompose()  # rip it out

        phones = []
        for match in PhoneNumberMatcher(soup.get_text(), "RU", leniency=Leniency.POSSIBLE):
            # find all possible phones to handle the case of Moscow phones without city code
            # this number not is_valid_number, but IS is_possible_number (see Leniency docs)
            if not is_valid_number(match.number):
                number = format_number(match.number, PhoneNumberFormat.NATIONAL)
                number = self.RE_EXTRA_CHARS.sub('', number)
                if not number.isdigit() or len(number) != 7:
                    continue
                number = f"+7{self.DEFAULT_CODE}{number}"
            else:
                number = format_number(match.number, PhoneNumberFormat.E164)
            if number.startswith("+7"):
                phones.append(number.replace("+7", "8"))
        return "OK", {"phones": ",".join(sorted(set(phones)))}
