import urlparse

from applications.search.crawler_frame import clean_up_dots_in_url


class CrawlingAnalytics:
    def __init__(self):
        self.subdomains = dict()
        self.invalid_urls = set()
        self.max_out_url_page = None

    def add_url(self, url):
        parsed_url = urlparse(url)
        if parsed_url.netloc != "":
            if not self.subdomains.has_key(parsed_url.netloc):
                self.subdomains[parsed_url.netloc] = set()
            self.subdomains[parsed_url.netloc].add(self.clean_up_url(url))

    # urls with same canonicalized urls are different
    def add_invalid_url(self, url):
        self.invalid_urls.add(url)

    # returns the first page with maximum out link
    def add_url_sub_url_count(self, url, sub_urls_count):
        if self.max_out_url_page is None:
            self.max_out_url_page = (url, sub_urls_count)
        else:
            if sub_urls_count > self.max_out_url_page[1]:
                self.max_out_url_page = (url, sub_urls_count)

    def clean_up_url(self, url):
        # Remove .. from URL
        url = clean_up_dots_in_url(url)
        # return canonicalize_url(url).lower()[:128]
        return (url).lower()

    def __unicode__(self):
        ret = "===================================\n"
        ret += "Subdomains Statistics\n"
        ret += "===================================\n"
        for subdomain, frequency in self.subdomains.items():
            ret += subdomain + " " + str(len(frequency)) + "\n"
        ret += "\n===================================\n"
        ret += "Invalid Links Statistics\n"
        ret += "===================================\n"
        ret += str(len(self.invalid_urls)) + " links\n"

        ret += "\n===================================\n"
        ret += "Max Out Link Statistics\n"
        ret += "===================================\n"

        if self.max_out_url_page:
            ret += self.max_out_url_page[0] + " : " + str(self.max_out_url_page[1]) + "\n"

        return ret

    def __str__(self):
        return self.__unicode__()