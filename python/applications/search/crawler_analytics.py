import os
from urlparse import urlparse

from applications.search.url_common import clean_up_dots_in_url


class CrawlingAnalytics:
    def __init__(self):
        self.subdomains = dict()
        self.invalid_urls = set()
        self.max_out_link_page = None
        self.report_dir = os.path.dirname(os.path.realpath(__file__)) + "/analytics/"

        if not os.path.exists(self.report_dir):
            os.makedirs(self.report_dir)

        self.load_subdomains()
        self.load_invalid_links()
        self.load_max_out_link_page()

    def load_subdomains(self):
        if os.path.exists(self.report_dir + "subdomains.txt"):
            with open(self.report_dir + "subdomains.txt") as f_all_subdomains:
                for subdomain in f_all_subdomains.read().splitlines():
                    self.subdomains[subdomain] = set()
                    if os.path.exists(self.report_dir + subdomain):
                        with open(self.report_dir + subdomain) as f_subdomain:
                            for url in f_subdomain.read().splitlines():
                                self.subdomains[subdomain].add(url)

    def load_invalid_links(self):
        if os.path.exists(self.report_dir + "invalid_urls.txt"):
            with open(self.report_dir + "invalid_urls.txt") as f:
                for url in f.read().splitlines():
                    self.invalid_urls.add(url)

    def load_max_out_link_page(self):
        if os.path.exists(self.report_dir + "max_out_link.txt"):
            with open(self.report_dir + "max_out_link.txt") as f:
                data = f.readline()
                if data:
                    self.max_out_link_page = tuple(data.split(","))

    # urls with same canonicalized urls are different
    def add_url(self, url):
        subdomain = urlparse(url).netloc
        url = self.clean_up_url(url)

        if subdomain != "":
            self.add_subdomain(subdomain)
            self.add_url_to_subdomain(subdomain, url)

    def add_subdomain(self, subdomain):
        if not self.subdomains.has_key(subdomain):
            self.subdomains[subdomain] = set()
            with open(self.report_dir + "subdomains.txt", "a") as f:
                f.write(subdomain + "\n")

    def add_url_to_subdomain(self, subdomain, url):
        if url not in self.subdomains[subdomain]:
            self.subdomains[subdomain].add(url)
            with open(self.report_dir + subdomain, "a") as f:
                f.write(url + "\n")

    # urls with same canonicalized urls are different
    def add_invalid_url(self, url):
        url = self.clean_up_url(url)
        if url not in self.invalid_urls:
            self.invalid_urls.add(url)
            with open(self.report_dir + "invalid_urls.txt", "a") as f:
                f.write(url + "\n")


    # returns the first page with maximum out link
    def add_url_out_link_count(self, url, out_links_count):
        if self.max_out_link_page is None or out_links_count > self.max_out_link_page[1]:
            self.max_out_link_page = (url, out_links_count)
            with open(self.report_dir + "max_out_link.txt", "w") as f:
                f.write(url + "," + out_links_count)

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

        if self.max_out_link_page:
            ret += self.max_out_link_page[0] + " : " + str(self.max_out_link_page[1]) + "\n"

        return ret

    def __str__(self):
        return self.__unicode__()