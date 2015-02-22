import storm
import urllib2
from bs4 import BeautifulSoup

class URLBolt(storm.BasicBolt):
    def process(self, tup):
        url = tup.values[0]
        try:
            html = urllib2.urlopen(url).read()

            soup = BeautifulSoup(html)
            urlText = soup.findAll({'title' : True, 'p' : True})

            if urlText:
                [storm.emit([t.string]) for t in urlText]
        except:
            pass

URLBolt().run()
