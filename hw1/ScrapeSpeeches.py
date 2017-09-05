from bs4 import BeautifulSoup
import requests,wget

class Parser(object):
    def __init__(self, url):
        self.url = url


    def download_page(self):
        print(self.url)
        page = requests.get(self.url)
        soup = BeautifulSoup(page.content)
#        print(soup.prettify())
        speech_list = soup.find(id="text")
        for speech in speech_list.find_all('a', href=True):
            print(url+'/'+ speech['href'])
            wget.download('http://stateoftheunion.onetwothree.net/texts/'+speech['href'], out='speeches')


if __name__ =='__main__':
    url = 'http://stateoftheunion.onetwothree.net/texts/index.html'
    parser = Parser(url)
    parser.download_page()
