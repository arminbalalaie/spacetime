import logging

from w3lib.url import canonicalize_url

from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from StringIO import StringIO
import atexit

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "55802153_82423652"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 Grad 55802153, 82423652"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(rawDatas):
    outputLinks = list()
    global crawler_analytics
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''

    for url_object in rawDatas:
        if url_object.is_redirected:
            url_prefix = url_object.final_url
        else:
            url_prefix = url_object.url

        # add subdomain analytics
        crawler_analytics.add_url(url_prefix)

        # add invalid URLs analytics
        # the URL is not valid if it does not have either scheme or netloc
        parsed = urlparse(url_prefix)
        if parsed.scheme not in ["http", "https"] or parsed.netloc is "":
            crawler_analytics.add_invalid_url(url_prefix)

        if url_object.http_code/100 <= 3:
            url_object.out_links = extract_links_from_content(url_object)
            url_object.bad_url = False
        else:
            print url_object.http_code
            url_object.bad_url = True
        outputLinks.extend(url_object.out_links)

    return outputLinks


def extract_links_from_content(url_object):
    global crawler_history
    global crawler_analytics
    ans = set()
    try:
        if url_object is not None:
            # Find final URL
            if url_object.is_redirected:
                url_prefix = url_object.final_url
            else:
                url_prefix = url_object.url

            crawler_history.add(url_prefix)

            # Parsing HTML content to extract links
            tree = html.fromstring(url_object.content)
            raw_links = tree.xpath('//a/@href')

            default_scheme = urlparse(url_prefix).scheme
            crawler_analytics.add_url_sub_url_count(url_prefix, len(raw_links))

            for link in raw_links:
                if len(link) < 2: continue
                # Make absolute links
                parsed = urlparse(link)
                if parsed.scheme not in ["","http", "https"]:
                    continue
                if link[0] is "/":
                    if link[1] is "/": # starts with //
                        absolute_url = default_scheme + ":" + link
                    else: # starts with /
                        absolute_url = default_scheme + "://" + urlparse(url_prefix).netloc + link
                elif parsed.netloc is "":
                    rslash_pos = url_prefix.rfind("/")
                    if rslash_pos is -1:
                        absolute_url = url_prefix + "/" + link
                    else:
                        absolute_url = url_prefix[:rslash_pos+1] + link
                else:
                    absolute_url = link

                ans.add(absolute_url)
    except:
        print "Parsing has been failed for " + url_prefix
        url_object.bad_url = True

    print ans
    return ans

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]) or not parsed.netloc:
        return False

    # Check if the URL is a trap
    if not crawler_history.is_url_in_frequency_limit(url):
        print "crawling trap! >> " + url
        return False

    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv|txt"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)


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
        pass
        return canonicalize_url(url).lower()


class CrawlingAnalytics:
    def __init__(self):
        self.subdomains = dict()
        self.invalid_urls = set()
        self.max_out_url_page = None

    def add_url(self, url):
        parsed_url = urlparse(url)
        if parsed_url.netloc != "":
            if not self.subdomains.has_key(parsed_url.netloc):
                self.subdomains[parsed_url.netloc] = 0
            self.subdomains[parsed_url.netloc] += 1

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

def report_analytics():
    global crawler_analytics

    print "Subdomains Statistics"
    print "==================================="
    for subdomain,frequency in crawler_analytics.subdomains.items():
        print subdomain,frequency

    print "Invalid Links Statistics"
    print "==================================="
    print len(crawler_analytics.invalid_urls) + "links"

    print "Max Out Link Statistics"
    print "==================================="
    print crawler_analytics.max_out_url_page[0], ":", crawler_analytics.max_out_url_page[1]
atexit.register(report_analytics)

# Initialize crawling history
crawler_history = CrawlerHistory(100,10)
crawler_analytics = CrawlingAnalytics()
