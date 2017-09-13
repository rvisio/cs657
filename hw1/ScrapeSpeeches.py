from bs4 import BeautifulSoup
import requests,wget, os

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

    def clean_pages(self):
        for filename in os.listdir('speeches'):
            with open('speeches/'+filename, 'r') as fin:
                soup = BeautifulSoup(fin.read())
                speech_text = soup.find(id="text")


                clean_text = speech_text.find_all('p')
                cleaned_string = ''
                for x in clean_text:
                    print (len(x))
                    print (type(x.get_text()))
                    cleaned_string = cleaned_string + x.get_text()


                with open('cleaned_speeches/' + filename, 'w') as f:
                    f.write(cleaned_string)





if __name__ =='__main__':
    url = 'http://stateoftheunion.onetwothree.net/texts/index.html'
    parser = Parser(url)
    #parser.download_page()
    parser.clean_pages()
