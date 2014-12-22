import os
import sys
import difflib

from selenium import webdriver
import selenium.webdriver.support.ui as selui

import abstracts


def search(query):
    driver = webdriver.Chrome()
    try:
        driver.get('https://scholar.google.com')
        wait = selui.WebDriverWait(driver, 10)
        search_el = wait.until(lambda driver: driver.find_element_by_name('q'))
        search_el.send_keys(query)
        search_el.submit()

        # get first 5 results (containing divs)
        abstracts = []
        results = wait.until(
            lambda driver: driver.find_elements_by_xpath(
                '//div[@class="gs_r"]'))

        for div in results:
            abstracts.append(parse_result(div))
    except Exception:
        print 'top-level exception'
        raise
        return []
    finally:
        driver.close()

    return [a for a in abstracts if a[1]]


def parse_result(div):
    a_tag = div.find_element_by_xpath('div[@class="gs_ri"][1]/h3/a')
    title = a_tag.text
    link = a_tag.get_attribute('href')
    abstract = abstracts.abstract_lookup(link)
    return (title, abstract)


def find_match(query):
    results = search(query)
    matcher = difflib.SequenceMatcher()
    matcher.set_seq1(test_query.lower())

    best_match = None
    best_ratio = -1
    for title, abstract in results:
        matcher.set_seq2(title.lower())
        similarity = matcher.quick_ratio()
        if similarity > best_ratio:
            best_match = (title, abstract)
            best_ratio = similarity

    if best_ratio > .8:
        return best_match
    else:
        return None


if __name__ == "__main__":
    test_query = 'Latent Dirichlet Allocation'
    match = find_match(test_query)
