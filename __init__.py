#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

__license__   = 'GPL v3'
__copyright__ = '2016, Gordon Yau <qunxyz@gmail.com>'
__docformat__ = 'restructuredtext en'

import socket, time, re
from threading import Thread
from Queue import Queue, Empty


from calibre import as_unicode
from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.sources.base import (Source, Option, fixcase,
                                                  fixauthors)
from calibre.ebooks.metadata.book.base import Metadata
from calibre.utils.localization import canonicalize_lang

class CaptchaError(Exception):
    pass

def parse_details_page(url, log, timeout, browser):
    from calibre.utils.cleantext import clean_ascii_chars
    from calibre.ebooks.chardet import xml_to_unicode
    import html5lib
    from lxml.html import tostring
    try:
        raw = browser.open_novisit(url, timeout=timeout).read().decode('gb18030').strip()
    except Exception as e:
        if callable(getattr(e, 'getcode', None)) and \
                        e.getcode() == 404:
            log.error('URL malformed: %r'%url)
            return
        attr = getattr(e, 'args', [None])
        attr = attr if attr else [None]
        if isinstance(attr[0], socket.timeout):
            msg = 'Amazon timed out. Try again later.'
            log.error(msg)
        else:
            msg = 'Failed to make details query: %r'%url
            log.exception(msg)
        return

    oraw = raw
    raw = raw
    raw = xml_to_unicode(raw, strip_encoding_pats=True, resolve_entities=True)[0]
    if '<title>404 - ' in raw:
        log.error('URL malformed: %r'%url)
        return

    try:
        root = html5lib.parse(raw, treebuilder='lxml',
                              namespaceHTMLElements=False)
    except:
        msg = 'Failed to parse amazon details page: %r'%url
        log.exception(msg)
        return

    errmsg = root.xpath('//*[@id="errorMessage"]')
    if errmsg:
        msg = 'Failed to parse amazon details page: %r'%url
        msg += tostring(errmsg, method='text', encoding=unicode).strip()
        log.error(msg)
        return

    from css_selectors import Select
    selector = Select(root)
    return oraw, root, selector

def parse_dang_id(root, log, url):
    try:
        link = root.xpath('//link[@rel="canonical" and @href]')
        for l in link:
            return l.get('href').rpartition('/')[-1].split('.')[0]
    except Exception:
        log.exception('Error parsing ASIN for url: %r'%url)


class Worker(Thread):  # Get details {{{

    '''
    Get book details from amazons book page in a separate thread
    '''

    def __init__(self, url, result_queue, browser, log, relevance,
                 plugin, timeout=20, testing=False, preparsed_root=None):
        Thread.__init__(self)
        self.preparsed_root = preparsed_root
        self.daemon = True
        self.testing = testing
        self.url, self.result_queue = url, result_queue
        self.log, self.timeout = log, timeout
        self.relevance, self.plugin = relevance, plugin
        self.browser = browser.clone_browser()
        self.cover_url = self.dang_id = self.isbn = None
        from lxml.html import tostring
        self.tostring = tostring

        self.english_months = [None, 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        self.months = {
            1: [u'1月'],
            2: [u'2月'],
            3: [u'3月'],
            4: [u'4月'],
            5: [u'5月'],
            6: [u'6月'],
            7: [u'7月'],
            8: [u'8月'],
            9: [u'9月'],
            10: [u'10月'],
            11: [u'11月'],
            12: [u'12月'],
        }

        # Base info block
        self.pd_desc_xpath = '//div[@id="detail_describe"]'
        self.pd_info_xpath = 'descendant::*[@class="messbox_info"]' # dangdang store
        self.pd_info_store_xpath = 'descendant::*[@class="book_messbox"]' # thirty store

        self.publisher_xpath = '//span[@dd_name="出版社"]/a'
        self.publisher_names = {'Publisher', '出版社'}

        self.publish_date_xpath = 'descendant::*[@dd_name="出版社"]/../span[starts-with(text(), "出版时间")]'

        self.language_names = {'Language', '语种'}

        self.tags_xpath = '//div[@class="breadcrumb"]/a'

        lm = {
            'eng': ('English', 'Englisch', 'Engels'),
            'zhn': ('Chinese', u'简体中文'),
        }
        self.lang_map = {}
        for code, names in lm.iteritems():
            for name in names:
                self.lang_map[name] = code

        self.series_pat = re.compile(
            r'''
            \|\s*              # Prefix
            (Series)\s*:\s*    # Series declaration
            (?P<series>.+?)\s+  # The series name
            \((Book)\s*    # Book declaration
            (?P<index>[0-9.]+) # Series index
            \s*\)
            ''', re.X)

    def delocalize_datestr(self, raw):
        if not self.months:
            return raw
        ans = raw.lower()
        for i, vals in self.months.iteritems():
            for x in vals:
                ans = ans.replace(x, self.english_months[i])
        ans = ans.replace(' de ', ' ')
        return ans

    def run(self):
        try:
            self.get_details()
        except:
            self.log.exception('get_details failed for url: %r'%self.url)

    def get_details(self):

        if self.preparsed_root is None:
            raw, root, selector = parse_details_page(self.url, self.log, self.timeout, self.browser)
        else:
            raw, root, selector = self.preparsed_root

        from css_selectors import Select
        self.selector = Select(root)
        self.parse_details(raw, root)

    def parse_details(self, raw, root):
        dang_id = parse_dang_id(root, self.log, self.url)
        if not dang_id and root.xpath('//form[@action="/errors/validateCaptcha"]'):
            raise CaptchaError('Amazon returned a CAPTCHA page, probably because you downloaded too many books. Wait for some time and try again.')
        if self.testing:
            import tempfile, uuid
            with tempfile.NamedTemporaryFile(prefix=(dang_id or str(uuid.uuid4()))+ '_',
                                             suffix='.html', delete=False) as f:
                f.write(raw)
            print ('Downloaded html for', dang_id, 'saved in', f.name)

        try:
            title = self.parse_title(root)
        except:
            self.log.exception('Error parsing title for url: %r'%self.url)
            title = None

        try:
            authors = self.parse_authors(root)
        except:
            self.log.exception('Error parsing authors for url: %r'%self.url)
            authors = []

        if not title or not authors or not dang_id:
            self.log.error('Could not find title/authors/dang_id for %r'%self.url)
            self.log.error('ASIN: %r Title: %r Authors: %r'%(dang_id, title,
                                                             authors))
            return

        mi = Metadata(title, authors)
        idtype = 'dang'
        mi.set_identifier(idtype, dang_id)
        self.dang_id = dang_id

        try:
            mi.comments = self.parse_comments(root, raw)
        except:
            self.log.exception('Error parsing comments for url: %r'%self.url)

        try:
            series, series_index = self.parse_series(root)
            if series:
                mi.series, mi.series_index = series, series_index
            elif self.testing:
                mi.series, mi.series_index = 'Dummy series for testing', 1
        except:
            self.log.exception('Error parsing series for url: %r'%self.url)

        try:
            mi.tags = self.parse_tags(root)
        except:
            self.log.exception('Error parsing tags for url: %r'%self.url)

        try:
            self.cover_url = self.parse_cover(root, raw)
        except:
            self.log.exception('Error parsing cover for url: %r'%self.url)
        mi.has_cover = bool(self.cover_url)

        pd = root.xpath(self.pd_desc_xpath)
        pd_info = root.xpath(self.pd_info_xpath)
        pd_info_store = root.xpath(self.pd_info_store_xpath)
        pd_desc = root.xpath(self.pd_desc_xpath)

        if pd_info or pd_info_store:
            try:
                isbn = self.parse_isbn(pd_info, pd_info_store, pd_desc)
                if isbn:
                    self.isbn = mi.isbn = isbn
            except:
                self.log.exception('Error parsing ISBN for url: %r'%self.url)

            if pd_info:
                pd_info = pd_info[0]
            else:
                pd_info = pd_info_store[0]


            try:
                mi.publisher = self.parse_publisher(pd_info)
            except:
                self.log.exception('Error parsing publisher for url: %r'%self.url)

            try:
                mi.pubdate = self.parse_pubdate(pd_info)
            except:
                self.log.exception('Error parsing publish date for url: %r'%self.url)


        else:
            self.log.warning('Failed to find product description for url: %r'%self.url)

        mi.source_relevance = self.relevance

        if self.dang_id:
            if self.isbn:
                self.plugin.cache_isbn_to_identifier(self.isbn, self.dang_id)
            if self.cover_url:
                self.plugin.cache_identifier_to_cover_url(self.dang_id,
                                                          self.cover_url)

        self.plugin.clean_downloaded_metadata(mi)

        self.result_queue.put(mi)

    def totext(self, elem):
        return self.tostring(elem, encoding=unicode, method='text').strip()

    def parse_title(self, root):
        h1 = root.xpath('//div[@class="name_info"]/h1')
        if h1:
            h1 = h1[0]
            # for child in h1.xpath('./*[contains(@class, "icon_name")]'):
            #     h1.remove(child)
            return self.totext(h1)
        tdiv = root.xpath('//img[@id="largePic"]')[0]
        actual_title = tdiv.get('alt')
        if actual_title:
            title = actual_title.strip()
        else:
            title = tdiv.strip()

        return title

    def parse_authors(self, root):
        matches = root.xpath('//span[@id="author"]/a')
        if matches:
            authors = [self.totext(x) for x in matches]
            return [a for a in authors if a]

        matches = root.xpath('//div[@class="book_messbox"]/div[1]/div[2]')
        if matches:
            authors = [self.totext(x) for x in matches]
            return [a for a in authors if a]

    def _render_comments(self, desc):
        from calibre.library.comments import sanitize_comments_html
        import html5lib
        # html5lib parsed noscript as CDATA

        desc = html5lib.parseFragment('<div>%s</div>' % (self.totext(desc).replace('textarea', 'div')), \
                                      treebuilder='lxml', namespaceHTMLElements=False)[0]
        matches = desc.xpath('descendant::*[contains(text(), "内容提要") \
            or contains(text(), "内容推荐") or contains(text(), "编辑推荐") \
            or contains(text(), "内容简介") or contains(text(), "基本信息")]/../*[self::p or self::div or self::span]')

        if matches:
            if len(matches)>1:
                desc = matches[-1]
                for item in matches:
                    content_len = len(self.totext(item))
                    if content_len > 50 and content_len < 200:
                        desc = item
                        break

        for c in desc.xpath('descendant::noscript'):
            c.getparent().remove(c)
        for c in desc.xpath('descendant::*[@class="seeAll" or'
                            ' @class="emptyClear" or @id="collapsePS" or'
                            ' @id="expandPS"]'):
            c.getparent().remove(c)
        #
        for a in desc.xpath('descendant::a[@href]'):
            del a.attrib['href']
            a.tag = 'span'
        desc = self.tostring(desc, method='text', encoding=unicode).strip()
        # return desc
        # Encoding bug in Amazon data U+fffd (replacement char)
        # in some examples it is present in place of '
        desc = desc.replace('\ufffd', "'")
        # remove all attributes from tags
        desc = re.sub(r'<([a-zA-Z0-9]+)\s[^>]+>', r'<\1>', desc)
        # Collapse whitespace
        desc = re.sub('\n+', '\n', desc)
        desc = re.sub(' +', ' ', desc)
        # Remove the notice about text referring to out of print editions
        desc = re.sub(r'(?s)<em>--This text ref.*?</em>', '', desc)
        # Remove comments
        desc = re.sub(r'(?s)<!--.*?-->', '', desc)
        return sanitize_comments_html(desc)

    def parse_comments(self, root, raw):
        from urllib import unquote
        ans = ''
        ns = root.xpath('//div[@class="descrip"]')

        if ns:
            ns = ns[0]
            if len(ns) == 0 and ns.text:
                import html5lib
                # html5lib parsed noscript as CDATA
                ns = html5lib.parseFragment('<div>%s</div>' % (ns.text), treebuilder='lxml', namespaceHTMLElements=False)[0]

            ans = self._render_comments(ns)

        return ans

    def parse_series(self, root):
        ans = (None, None)

        # This is found on the paperback/hardback pages for books on amazon.com
        series = root.xpath('//div[@data-feature-name="seriesTitle"]')
        if series:
            series = series[0]
            spans = series.xpath('./span')
            if spans:
                raw = self.tostring(spans[0], encoding=unicode, method='text', with_tail=False).strip()
                m = re.search('\s+([0-9.]+)$', raw.strip())
                if m is not None:
                    series_index = float(m.group(1))
                    s = series.xpath('./a[@id="series-page-link"]')
                    if s:
                        series = self.tostring(s[0], encoding=unicode, method='text', with_tail=False).strip()
                        if series:
                            ans = (series, series_index)
        # This is found on Kindle edition pages on amazon.com
        if ans == (None, None):
            for span in root.xpath('//div[@id="aboutEbooksSection"]//li/span'):
                text = (span.text or '').strip()
                m = re.match('Book\s+([0-9.]+)', text)
                if m is not None:
                    series_index = float(m.group(1))
                    a = span.xpath('./a[@href]')
                    if a:
                        series = self.tostring(a[0], encoding=unicode, method='text', with_tail=False).strip()
                        if series:
                            ans = (series, series_index)
        if ans == (None, None):
            desc = root.xpath('//div[@id="ps-content"]/div[@class="buying"]')
            if desc:
                raw = self.tostring(desc[0], method='text', encoding=unicode)
                raw = re.sub(r'\s+', ' ', raw)
                match = self.series_pat.search(raw)
                if match is not None:
                    s, i = match.group('series'), float(match.group('index'))
                    if s:
                        ans = (s, i)
        if ans[0]:
            ans = (re.sub(r'\s+Series$', '', ans[0]).strip(), ans[1])
            ans = (re.sub(r'\(.+?\s+Series\)$', '', ans[0]).strip(), ans[1])
        return ans

    def parse_tags(self, root):
        ans = []
        exclude_tokens = {'kindle', 'a-z'}
        exclude = {'special features', 'by authors', 'authors & illustrators', 'books', 'new; used & rental textbooks'}
        seen = set()

        for a in root.xpath(self.tags_xpath):
            raw = (a.text or '').strip().replace(',', ';').replace('/', ';').replace('>', ';')

            lraw = icu_lower(raw)
            tokens = frozenset(lraw.split())
            if raw and lraw not in exclude and not tokens.intersection(exclude_tokens) and lraw not in seen:
                ans.append(raw)
                seen.add(lraw)

        return ans

    def parse_cover(self, root, raw=b""):
        matches = root.xpath('//img[@id="largePic"]')
        if matches:
            src = matches[0].get('src')
            if 'blank.gif' not in src:
                return src

            src = matches[0].get('wsrc')
            if 'blank.gif' not in src:
                return src

    def parse_new_details(self, root, mi, non_hero):
        table = non_hero.xpath('descendant::table')[0]
        for tr in table.xpath('descendant::tr'):
            cells = tr.xpath('descendant::td')
            if len(cells) == 2:
                name = self.totext(cells[0])
                val = self.totext(cells[1])
                if not val:
                    continue
                if name in self.language_names:
                    ans = self.lang_map.get(val, None)
                    if not ans:
                        ans = canonicalize_lang(val)
                    if ans:
                        mi.language = ans
                elif name in self.publisher_names:
                    pub = val.partition(';')[0].partition('(')[0].strip()
                    if pub:
                        mi.publisher = pub
                    date = val.rpartition('(')[-1].replace(')', '').strip()
                    try:
                        from calibre.utils.date import parse_only_date
                        date = self.delocalize_datestr(date)
                        mi.pubdate = parse_only_date(date, assume_utc=True)
                    except:
                        self.log.exception('Failed to parse pubdate: %s' % val)
                elif name in {'ISBN', 'ISBN-10', 'ISBN-13'}:
                    ans = check_isbn(val)
                    if ans:
                        self.isbn = mi.isbn = ans

    def parse_isbn(self, pd, pd_info, pd_desc):
        pd_xpath = 'descendant::div[@class="show_info_left" and (contains(text(), "I") and \
                contains(text(), "S") and contains(text(), "B") and contains(text(), "N")) or (\
                contains(text(), "Ｉ") and contains(text(), "Ｓ") and contains(text(), "Ｂ") and \
                contains(text(), "Ｎ"))]/../div'
        if pd:
            matches = pd[0].xpath(pd_xpath)

            if matches:
                ans = check_isbn(self.totext(matches[1]).strip())
                if ans:
                    return ans
                else:
                    self.log.info('wrong isbn: %s'%self.totext(matches[1]).strip())


        if pd_info:
            matches = pd_info[0].xpath(pd_xpath)
            if matches:
                ans = check_isbn(self.totext(matches[1]).strip())
                if ans:
                    return ans
                else:
                    self.log.info('wrong isbn: %s'%self.totext(matches[1]).strip())

        if pd_desc:
            matches = pd_desc[0].xpath('descendant::*[starts-with(text(), "国际标准书号ISBN")]')
            if matches:
                matches = re.split(r'(:|：|\n)+', self.totext(matches[0]))
                if len(matches)>1:
                    ans = check_isbn(matches[-1].strip())
                    if ans:
                        return ans
                    else:
                        self.log.info('wrong isbn: %s'%self.totext(matches[-1].strip()))

    def parse_publisher(self, pd):
        matches = pd.xpath(self.publisher_xpath)

        if matches:
            return self.totext(matches[0])

        matches = pd.xpath('descendant::div[@class="show_info_left" and contains(text(), "出") and \
        contains(text(), "版") and contains(text(), "社")]/../div')

        if matches:
            return self.totext(matches[1])


    def parse_pubdate(self, pd):
        matches = pd.xpath(self.publish_date_xpath)
        date = None
        if matches:
            date = self.totext(matches[0])

        if not matches:
            matches = pd.xpath('//div[@class="show_info_left" and contains(text(), "出版时间")]/../div')

            if len(matches)>1:
                date = self.totext(matches[1])

        if date:
            from calibre.utils.date import parse_only_date

            date = date.replace('年', '/').replace('月', '/').replace('日', '') \
                .replace('-', '/').replace('出版时间', '').replace(':', '').strip()
            if date.endswith('/'):
                date = "%s1"%date

            date = self.delocalize_datestr(date)
            return parse_only_date(date, assume_utc=True)

    def parse_language(self, pd):
        for x in reversed(pd.xpath(self.language_xpath)):
            if x.tail:
                raw = x.tail.strip().partition(',')[0].strip()
                ans = self.lang_map.get(raw, None)
                if ans:
                    return ans
                ans = canonicalize_lang(ans)
                if ans:
                    return ans
# }}}

class Dang(Source):

    name = 'DangDang'
    description = _('Downloads metadata and covers from dangdang.com')

    capabilities = frozenset(['identify', 'cover'])
    touched_fields = frozenset(['title', 'authors', 'identifier:dang',
                                'rating', 'comments', 'publisher', 'pubdate',
                                'languages', 'series', 'tags'])
    has_html_comments = True
    supports_gzip_transfer_encoding = True
    prefer_results_with_isbn = False
    auto_trim_covers = True

    def __init__(self, *args, **kwargs):
        Source.__init__(self, *args, **kwargs)
        self.set_dang_id_touched_fields()

    def test_fields(self, mi):
        '''
        Return the first field from self.touched_fields that is null on the
        mi object
        '''
        for key in self.touched_fields:
            if key.startswith('identifier:'):
                key = key.partition(':')[-1]
                if not mi.has_identifier(key):
                    return 'identifier: ' + key
            elif mi.is_null(key):
                return key

    @property
    def user_agent(self):
        # IE 11 - windows 7
        return 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko'

    def save_settings(self, *args, **kwargs):
        Source.save_settings(self, *args, **kwargs)
        self.set_dang_id_touched_fields()

    def set_dang_id_touched_fields(self):
        ident_name = "identifier:dang"
        tf = [x for x in self.touched_fields if not
        x.startswith('identifier:dang')] + [ident_name]
        self.touched_fields = frozenset(tf)

    def get_dang_id(self, identifiers):
        for key, val in identifiers.iteritems():
            key = key.lower()
            if key == 'dang':
                return val
        return None

    def _get_book_url(self, identifiers):  # {{{
        dang_id = self.get_dang_id(identifiers)
        if dang_id:
            url = 'http://product.dangdang.com/%s.html'%(dang_id)
            return dang_id, url

    def get_book_url(self, identifiers):
        ans = self._get_book_url(identifiers)
        if ans is not None:
            return ans[1:]

    def get_book_url_name(self, idtype, idval, url):
        if idtype == 'amazon':
            return self.name
        return 'A' + idtype.replace('_', '.')[1:]
    # }}}

    def clean_downloaded_metadata(self, mi):
        docase = (
            mi.language == 'zhn'
        )
        if mi.title and docase:
            # Remove series information from title
            m = re.search(r'\S+\s+(\(.+?\s+Book\s+\d+\))$', mi.title)
            if m is not None:
                mi.title = mi.title.replace(m.group(1), '').strip()
            mi.title = fixcase(mi.title)
        mi.authors = fixauthors(mi.authors)
        if mi.tags and docase:
            mi.tags = list(map(fixcase, mi.tags))
        mi.isbn = check_isbn(mi.isbn)
        if mi.series and docase:
            mi.series = fixcase(mi.series)
        if mi.title and mi.series:
            for pat in (r':\s*Book\s+\d+\s+of\s+%s$', r'\(%s\)$', r':\s*%s\s+Book\s+\d+$'):
                pat = pat % re.escape(mi.series)
                q = re.sub(pat, '', mi.title, flags=re.I).strip()
                if q and q != mi.title:
                    mi.title = q
                    break

    def create_query(self, log, title=None, authors=None, identifiers={}):  # {{{
        from urllib import urlencode

        dang_id = self.get_dang_id(identifiers)

        # See the amazon detailed search page to get all options
        q = {'medium': '01', # ebook is 22
             'category_path': '01.00.00.00.00.00',
             }

        q['sort_type'] = 'sort_score_desc'

        isbn = check_isbn(identifiers.get('isbn', None))

        if dang_id is not None:
            url = 'http://product.dangdang.com/%s.html'%dang_id
            return url
        elif isbn is not None:
            q['key4'] = isbn
        else:
            # Only return book results
            if title:
                title_tokens = list(self.get_title_tokens(title))
                if title_tokens:
                    q['key1'] = ' '.join(title_tokens)
            if authors:
                author_tokens = self.get_author_tokens(authors,
                                                       only_first_author=True)
                if author_tokens:
                    q['key2'] = ' '.join(author_tokens)

        if not ('key1' in q or 'key2' in q or
                    ('key4' in q)):
            # Insufficient metadata to make an identify query
            return None, None

        encode_to='gb18030'
        encoded_q = dict([(x.encode(encode_to, 'ignore'), y.encode(encode_to,
                                                                   'ignore')) for x, y in
                          q.iteritems()])
        url = 'http://search.dangdang.com/?%s'%urlencode(encoded_q)

        return url

    # }}}

    def get_cached_cover_url(self, identifiers):  # {{{
        url = None
        dang_id = self.get_dang_id(identifiers)
        if dang_id is None:
            isbn = identifiers.get('isbn', None)
            if isbn is not None:
                dang_id = self.cached_isbn_to_identifier(isbn)

        if dang_id is not None:
            url = self.cached_identifier_to_cover_url(dang_id)

        return url
    # }}}

    def parse_results_page(self, root):  # {{{
        from lxml.html import tostring

        matches = []

        def title_ok(title):
            title = title.lower()
            bad = ['bulk pack', '[audiobook]', '[audio cd]', '(a book companion)', '( slipcase with door )', ': free sampler']
            for x in bad:
                if x in title:
                    return False
            # if title and title[0] in '[{' and re.search(r'\(\s*author\s*\)', title) is not None:
            #     # Bad entries in the catalog
            #     return False
            return True

        for a in root.xpath(r'//li[starts-with(@class, "line")]//a[@href and contains(@name, "itemlist-picture")]'):
            # title = a.get('title')
            # if title_ok(title):
            url = a.get('href')
            if url.startswith('/'):
                url = 'http://product.dangdang.com/%s' % (url)
            matches.append(url)

        # Keep only the top 5 matches as the matches are sorted by relevance by
        # Amazon so lower matches are not likely to be very relevant
        return matches[:5]
    # }}}

    def fetch_raw(self, log, url, br, testing,  # {{{
                  identifiers={}, timeout=30):
        from calibre.utils.cleantext import clean_ascii_chars
        from calibre.ebooks.chardet import xml_to_unicode
        from lxml.html import tostring
        import html5lib
        try:
            raw = br.open_novisit(url, timeout=timeout).read().decode('gb18030').strip()
        except Exception as e:
            if callable(getattr(e, 'getcode', None)) and \
                            e.getcode() == 404:
                log.error('Query malformed: %r'%url)
                return
            attr = getattr(e, 'args', [None])
            attr = attr if attr else [None]
            if isinstance(attr[0], socket.timeout):
                msg = _('DangDang timed out. Try again later.')
                log.error(msg)
            else:
                msg = 'Failed to make identify query: %r'%url
                log.exception(msg)
            return as_unicode(msg)

        raw = clean_ascii_chars(xml_to_unicode(raw,
                                               strip_encoding_pats=True, resolve_entities=True)[0])

        if testing:
            import tempfile
            with tempfile.NamedTemporaryFile(prefix='dangdang_results_',
                                             suffix='.html', delete=False) as f:
                f.write(raw.encode('utf-8'))
            print ('Downloaded html for results page saved in', f.name)

        matches = []
        found = '<title>对不起，您要访问的页面暂时没有找到' not in raw

        if found:
            try:
                root = html5lib.parse(raw, treebuilder='lxml',
                                      namespaceHTMLElements=False)
            except:
                msg = 'Failed to parse DangDang page for query: %r'%url
                log.exception(msg)
                return msg

        return found, root

    def identify(self, log, result_queue, abort, title=None, authors=None,  # {{{
                 identifiers={}, timeout=30):
        '''
        Note this method will retry without identifiers automatically if no
        match is found with identifiers.
        '''
        from calibre.utils.cleantext import clean_ascii_chars
        from calibre.ebooks.chardet import xml_to_unicode
        from lxml.html import tostring
        import html5lib

        testing = getattr(self, 'running_a_test', False)
        br = self.browser

        udata = self._get_book_url(identifiers)
        if udata is not None:
            # Try to directly get details page instead of running a search
            dang_id, durl = udata
            preparsed_root = parse_details_page(durl, log, timeout, br)
            if preparsed_root is not None:
                qdang_id = parse_dang_id(preparsed_root[1], log, durl)
                if qdang_id == dang_id:
                    w = Worker(durl, result_queue, br, log, 0, self, testing=testing, preparsed_root=preparsed_root)
                    try:
                        w.get_details()
                        return
                    except Exception:
                        log.exception('get_details failed for url: %r'%durl)

        query = self.create_query(log, title=title, authors=authors,
                                          identifiers=identifiers)
        if query is None:
            log.error('Insufficient metadata to construct query')
            return
        if testing:
            print ('Using user agent for dangdang: %s'%self.user_agent)
        #####
        if query.startswith('http://product.'):
            found = True
            matches = [query]
        else:
            found, root = self.fetch_raw(log, query, br, testing)

            if found:
                matches = self.parse_results_page(root)

        if abort.is_set():
            return

        if not matches:
            if identifiers and title and authors:
                log('No matches found with identifiers, retrying using only'
                    ' title and authors. Query: %r'%query)
                return self.identify(log, result_queue, abort, title=title,
                                     authors=authors, timeout=timeout)
            log.error('No matches found with query: %r'%query)
            return

        workers = [Worker(url, result_queue, br, log, i, self,
                          testing=testing) for i, url in enumerate(matches)]

        for w in workers:
            w.start()
            # Don't send all requests at the same time
            time.sleep(0.1)

        while not abort.is_set():
            a_worker_is_alive = False
            for w in workers:
                w.join(0.2)
                if abort.is_set():
                    break
                if w.is_alive():
                    a_worker_is_alive = True
            if not a_worker_is_alive:
                break

        return None
    # }}}

    def download_cover(self, log, result_queue, abort,  # {{{
                       title=None, authors=None, identifiers={}, timeout=30, get_best_cover=False):
        cached_url = self.get_cached_cover_url(identifiers)
        if cached_url is None:
            log.info('No cached cover found, running identify')
            rq = Queue()
            self.identify(log, rq, abort, title=title, authors=authors,
                          identifiers=identifiers)
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(key=self.identify_results_keygen(
                title=title, authors=authors, identifiers=identifiers))
            for mi in results:
                cached_url = self.get_cached_cover_url(mi.identifiers)
                if cached_url is not None:
                    break
        if cached_url is None:
            log.info('No cover found')
            return

        if abort.is_set():
            return
        br = self.browser
        log('Downloading cover from:', cached_url)
        try:
            cdata = br.open_novisit(cached_url, timeout=timeout).read()

            result_queue.put((self, cdata))
        except:
            log.exception('Failed to download cover from:', cached_url)
            # }}}

if __name__ == '__main__':  # tests {{{
    # To run these test use: calibre-debug src/calibre/ebooks/metadata/sources/amazon.py
    from calibre.ebooks.metadata.sources.test import (test_identify_plugin,
                                                      isbn_test, title_test, authors_test, comments_test, series_test)
    com_tests = [  # {{{

        (   # Paperback with series
            {'identifiers':{'amazon':'1423146786'}},
            [title_test('The Heroes of Olympus, Book Five The Blood of Olympus', exact=True), series_test('Heroes of Olympus', 5)]
        ),

        (   # Kindle edition with series
            {'identifiers':{'amazon':'B0085UEQDO'}},
            [title_test('Three Parts Dead', exact=True), series_test('Craft Sequence', 1)]
        ),

        (   # A kindle edition that does not appear in the search results when searching by ASIN
            {'identifiers':{'amazon':'B004JHY6OG'}},
            [title_test('The Heroes: A First Law Novel (First Law World 2)', exact=True)]
        ),

        (  # + in title and uses id="main-image" for cover
            {'identifiers':{'amazon':'1933988770'}},
            [title_test('C++ Concurrency in Action: Practical Multithreading', exact=True)]
        ),


        (  # noscript description
            {'identifiers':{'amazon':'0756407117'}},
            [title_test(
                "Throne of the Crescent Moon"),
                comments_test('Makhslood'), comments_test('Dhamsawaat'),
            ]
        ),

        (  # Different comments markup, using Book Description section
            {'identifiers':{'amazon':'0982514506'}},
            [title_test(
                "Griffin's Destiny: Book Three: The Griffin's Daughter Trilogy",
                exact=True),
                comments_test('Jelena'), comments_test('Ashinji'),
            ]
        ),

        (  # # in title
            {'title':'Expert C# 2008 Business Objects',
             'authors':['Lhotka']},
            [title_test('Expert C# 2008 Business Objects'),
             authors_test(['Rockford Lhotka'])
             ]
        ),

        (  # Description has links
            {'identifiers':{'isbn': '9780671578275'}},
            [title_test('A Civil Campaign: A Comedy of Biology and Manners',
                        exact=True), authors_test(['Lois McMaster Bujold'])
             ]

        ),

        (  # Sophisticated comment formatting
            {'identifiers':{'isbn': '9781416580829'}},
            [title_test('Angels & Demons - Movie Tie-In: A Novel',
                        exact=True), authors_test(['Dan Brown'])]
        ),

        (  # No specific problems
            {'identifiers':{'isbn': '0743273567'}},
            [title_test('The great gatsby', exact=True),
             authors_test(['F. Scott Fitzgerald'])]
        ),

    ]  # }}}

    de_tests = [  # {{{
        (
            {'identifiers':{'isbn': '9783453314979'}},
            [title_test('Die letzten Wächter: Roman',
                        exact=False), authors_test(['Sergej Lukianenko', 'Christiane Pöhlmann'])
             ]

        ),

        (
            {'identifiers':{'isbn': '3548283519'}},
            [title_test('Wer Wind Sät: Der Fünfte Fall Für Bodenstein Und Kirchhoff',
                        exact=False), authors_test(['Nele Neuhaus'])
             ]

        ),
    ]  # }}}

    it_tests = [  # {{{
        (
            {'identifiers':{'isbn': '8838922195'}},
            [title_test('La briscola in cinque',
                        exact=True), authors_test(['Marco Malvaldi'])
             ]

        ),
    ]  # }}}

    fr_tests = [  # {{{
        (
            {'identifiers':{'isbn': '2221116798'}},
            [title_test('L\'étrange voyage de Monsieur Daldry',
                        exact=True), authors_test(['Marc Levy'])
             ]

        ),
    ]  # }}}

    es_tests = [  # {{{
        (
            {'identifiers':{'isbn': '8483460831'}},
            [title_test('Tiempos Interesantes',
                        exact=True), authors_test(['Terry Pratchett'])
             ]

        ),
    ]  # }}}

    jp_tests = [  # {{{
        (  # Adult filtering test
            {'identifiers':{'isbn':'4799500066'}},
            [title_test(u'Ｂｉｔｃｈ Ｔｒａｐ'),]
        ),

        (  # isbn -> title, authors
            {'identifiers':{'isbn': '9784101302720'}},
            [title_test(u'精霊の守り人',
                        exact=True), authors_test([u'上橋 菜穂子'])
             ]
        ),
        (  # title, authors -> isbn (will use Shift_JIS encoding in query.)
            {'title': u'考えない練習',
             'authors': [u'小池 龍之介']},
            [isbn_test('9784093881067'), ]
        ),
    ]  # }}}

    br_tests = [  # {{{
        (
            {'title':'Guerra dos Tronos'},
            [title_test('A Guerra dos Tronos - As Crônicas de Gelo e Fogo',
                        exact=True), authors_test(['George R. R. Martin'])
             ]

        ),
    ]  # }}}

    nl_tests = [  # {{{
        (
            {'title':'Freakonomics'},
            [title_test('Freakonomics',
                        exact=True), authors_test(['Steven Levitt & Stephen Dubner & R. Kuitenbrouwer & O. Brenninkmeijer & A. van Den Berg'])
             ]

        ),
    ]  # }}}

    def do_test(domain, start=0, stop=None):
        tests = globals().get(domain+'_tests')
        if stop is None:
            stop = len(tests)
        tests = tests[start:stop]
        test_identify_plugin(Amazon.name, tests, modify_plugin=lambda
            p:(setattr(p, 'testing_domain', domain), setattr(p, 'touched_fields', p.touched_fields - {'tags'})))

    do_test('com')

    # do_test('de')

    # }}}