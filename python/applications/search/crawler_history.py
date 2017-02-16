from w3lib.url import canonicalize_url

from applications.search.url_common import clean_up_dots_in_url


class CrawlerHistory:
    crawler_history = []
    crawler_history_occurrence = {}

    def __init__(self, size, max_frequency):
        self.crawler_size = size
        self.max_frequency = max_frequency

    def add(self, url):
        url = self.clean_up_url(url)
        # Add url to queue
        self.crawler_history.append(url)
        # Update url occurrence frequency
        if not self.crawler_history_occurrence.has_key(url):
            self.crawler_history_occurrence[url] = 0
        self.crawler_history_occurrence[url] += 1

        # Remove the last item from queue and frequency dict
        if len(self.crawler_history) > self.crawler_size:
            self.crawler_history_occurrence[self.crawler_history[0]] -= 1
            if self.crawler_history_occurrence[self.crawler_history[0]] == 0:
                self.crawler_history_occurrence.pop(self.crawler_history[0], None)
            self.crawler_history.pop(0)

    def __contains__(self, url):
        return self.crawler_history_occurrence.has_key(self.clean_up_url(url))

    def is_url_in_frequency_limit(self, url):
        return self.max_frequency >= self.crawler_history_occurrence.get(self.clean_up_url(url), 0)

    def clean_up_url(self, url):
        # Remove Query String
        url = url[:url.rfind("?")]
        # Remove .. from URL
        url = clean_up_dots_in_url(url)
        return canonicalize_url(url).lower()[:128]