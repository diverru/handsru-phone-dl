from unittest import TestCase

from downloader.phone_parser import PhoneParser


class TestPhoneParser(TestCase):
    parser = PhoneParser()

    def test_process_different_formats(self):
        phones = [
            ("+79161234567", "89161234567"),
            ("89161234567", "89161234567"),
            ("8(916)1234567", "89161234567"),
            ("8(916)123-45-67", "89161234567"),
            ("8-920-123-45-67", "89201234567"),
            ("8 (920) 123-45-67", "89201234567"),
            ("8 (920) 123 45 67", "89201234567"),
            ("8 920 123 45 67", "89201234567"),
        ]
        for phone, expected in phones:
            status, result = self.parser.process_content(
               f"<div>{phone}</div>"
            )
            self.assertEqual('OK', status)
            self.assertEqual(expected, result["phones"])

    def test_removes_scripts(self):
        status, result = self.parser.process_content(
            "<div><script>8(916)123-45-67</script></div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('', result["phones"])

    def test_removes_css(self):
        status, result = self.parser.process_content(
            "<div><style>8(916)123-45-67</style></div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('', result["phones"])

    def test_default_code(self):
        status, result = self.parser.process_content(
            "<div>123-45-67</div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('84951234567', result["phones"])

    def test_repeated_phones(self):
        status, result = self.parser.process_content(
            "<div>8(916)123-45-67, 8-916-123-45-67</div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('89161234567', result["phones"])

    def test_multiple_phones(self):
        status, result = self.parser.process_content(
            "<div>8(916)123-45-67, 8-916-765-43-21</div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('89161234567,89167654321', result["phones"])

    def test_multiple_phones_different_tags(self):
        status, result = self.parser.process_content(
            "<div><span>8(916)123-45-67</span> <span>8-916-765-43-21</span></div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('89161234567,89167654321', result["phones"])

    def test_regional_code(self):
        status, result = self.parser.process_content(
            "<div>8(4855)55-56-70</div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('84855555670', result["phones"])

    def test_short_number_no_code(self):
        status, result = self.parser.process_content(
            "<div>55-56-70</div>"
        )
        self.assertEqual('OK', status)
        self.assertEqual('', result["phones"])
