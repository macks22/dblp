import re
import os
import sys
import multiprocessing as mp

import lxml.html
import requests
from selenium import webdriver
import selenium.webdriver.support.ui as selui


def ieee_lookup(root):
    result = root.xpath('//div[@id="articleDetails"]/div/div/p/text()')
    return result[0] if result else None

def springer_lookup(root):
    result = root.xpath('//div[@class="abstract-content formatted"]/p/text()')
    if result:
        return result[0]

    # follow additional info link
    a_taglist = root.xpath(
        '//dd[@id="abstract-about-additional-links"]/ul/li/a')
    if a_taglist:
        link = a_taglist[0].get('href')
        r = requests.get(link)
        subroot = lxml.html.fromstring(r.content)
        info = subroot.xpath(
            '//div[@class="productTab"]/div/div/div/p/text()')
        if info:
            return ' '.join([text.strip() for text in info])

    return None


def siam_lookup(root):
    result = root.xpath('//div[@class="abstractSection"]/p/text()')
    return ''.join(result) if result else None

def scitepress_lookup(root):
    result = root.xpath('//span[@id="Main_LabelAbstract"]/text()')
    return result[0] if result else None

def ssrn_lookup(root):
    result = root.xpath('//div[@id="abstract"]/text()')
    return ' '.join(result) if result else None

def wiley_lookup(root):
    keywords = root.xpath('//div[@class="keywordLists"]/ul/li/text()')
    summary = root.xpath('//div[@id="abstract"]/div/ul/li/div/p/text()')
    if keywords and summary:
        return ' '.join(keywords) + ' '.join(summary)
    elif keywords:
        return ' '.join(keywords)
    elif summary:
        return ' '.join(summary)
    else:
        result = root.xpath('//div[@id="abstract"]/div/p/text()')
        return ''.join(result) if result else None

def sciencedirect_lookup(url):
    driver = webdriver.Chrome()
    driver.get(url)
    wait = selui.WebDriverWait(driver, 10)
    try:
        div = wait.until(lambda driver: driver.find_element_by_id('frag_2'))
        p = div.find_element_by_xpath('div/p')
        result = p.text
    except Exception: #NoSuchElementException
        result = None

    driver.close()
    return result

def degruyter_lookup(root):
    result = root.xpath('//div[@class="articleBody_abstract"]/p/text()')
    return result[0] if result else None

def projecteuclid_lookup(root):
    result = root.xpath('//div[@id="abstract"]/div/p/text()')
    return ' '.join([text.strip() for text in result]) if result else None

def spie_lookup(root):
    result = root.xpath('//span[@class="Abstract"]/p/text()')
    return result[0] if result else None

def jmlr_lookup(root):
    result = root.xpath(
        '//div[@id="content"]/text()|//div[@id="content"]/i/text()')
    return ' '.join([t.strip() for t in result if t.strip()]).replace('\n','')

def acm_lookup(link):
    driver = webdriver.Chrome()
    driver.get(link)
    wait = selui.WebDriverWait(driver, 10)
    try:
        div = wait.until(
            lambda driver: driver.find_element_by_xpath(
                '//div[@id="abstract"]/div/div'))
        abstract = div.text
    except Exception:
        abstract = None
    finally:
        driver.close()
    return abstract

def nips_lookup(root):
    result = root.xpath('//p[@class="abstract"]/text()')
    return result[0] if result else None


def abstract_lookup(link):
    print link
    r = requests.get(link)
    root = lxml.html.fromstring(r.content)
    domain = r.url

    if 'ieee' in domain:
        abstract = ieee_lookup(root)
    elif 'springer' in domain:
        abstract = springer_lookup(root)
    elif 'siam' in domain:
        abstract = siam_lookup(root)
    elif 'scitepress' in domain:
        abstract = scitepress_lookup(root)
    elif 'ssrn' in domain:
        abstract = ssrn_lookup(root)
    elif 'wiley' in domain:
        abstract = wiley_lookup(root)
    elif 'sciencedirect' in domain:
        abstract = sciencedirect_lookup(link)
    elif 'degruyter' in domain:
        abstract = degruyter_lookup(root)
    elif 'projecteuclid' in domain:
        abstract = projecteuclid_lookup(root)
    elif 'spiedigitallibrary' in domain:
        abstract = spie_lookup(root)
    elif 'jmlr' in domain:
        abstract = jmlr_lookup(root)
    elif 'acm' in domain:
        abstract = acm_lookup(link)
    elif 'nips' in domain:
        abstract = nips_lookup(root)
    else:
        print 'unknown domain: %s (%s)' % (domain, link)
        abstract = None

    if abstract is None:
        print 'no abstract resolved: %s (%s)' % (domain, link)
    return abstract


URL = "http://search.crossref.org"
def make_query(search_string):
    params = {'q': search_string}
    r = requests.get(URL, params=params)
    return lxml.html.fromstring(r.content)


def extract_record(td):
    title_tag = td.xpath('p[@class="lead"]')[0]
    title = title_tag.text_content().strip()
    if '\n' in title:
        title = title.split('\n')[-1].strip()

    try:
        info_tag = td.xpath('p[@class="extra"]')[0]
    except IndexError:
        return None

    info = info_tag.text_content().strip()
    metadata = [text.strip() for text in info.split('\n')]
    cleaned = [text for text in metadata if text]
    if len(cleaned) < 5:
        return None

    pub_type = cleaned[0]
    date = cleaned[2]
    venue = cleaned[4]

    # date may contain month; we don't care about this
    parts = date.split()
    if len(parts) > 1:
        year = filter(lambda c: c.isdigit(), parts[-1])
    else:
        year = filter(lambda c: c.isdigit(), date)

    author_text = td.xpath('p[@class="expand"]/text()')
    if author_text:
        author_list = author_text[0][9:].split(',')
    else:
        author_list = []

    links = filter(lambda s: 'http' in s, td.xpath('div/div/a/text()'))
    return {
        'title': title,
        'type': pub_type,
        'year': int(year) if year else None,
        'venue': venue,
        'authors': author_list,
        'link': links[0].strip() if links else None
    }


def extract_records(root):
    data_rows = root.xpath('//td[@class="item-data"]')
    return [extract_record(td) for td in data_rows]

def query(title):
    root = make_query(title)
    return [record for record in extract_records(root) if record]


if __name__ == "__main__":
    test_titles = ["Low-Rank Matrix Approximation", "Collaborative Filtering"]
    pool = mp.Pool()
    results = pool.map(query, test_titles)
    records = reduce(lambda x, y: x + y, results)
    dois = [r['link'] for r in records]
    look = abstract_lookup
    abstracts = pool.map(look, dois)
