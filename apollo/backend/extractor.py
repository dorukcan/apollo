import html

import lxml.html


def build_soup(content):
    parser = lxml.html.HTMLParser()
    return lxml.html.document_fromstring(content, parser=parser)


# query
def q(_item, _selector):
    return _item.cssselect(_selector)


######################################################


# query single
def qs(_item, _selector, position=0):
    q_cache = q(_item, _selector)

    return q_cache[position] if q_cache else build_soup('<html></html>')


# query single text
def qst(_item, _selector, do_strip=False):
    txt = qs(_item, _selector).text_content()

    if do_strip:
        txt = " ".join(txt.split())

    return txt


# query single html
def qsh(_item, _selector):
    return html.escape(str(qs(_item, _selector)))


# query single url
def qsu(_item, _selector):
    return qs(_item, _selector).get("href")


# query single src
def qss(_item, _selector):
    return qs(_item, _selector).get("src")
