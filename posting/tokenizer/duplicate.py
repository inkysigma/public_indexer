from lxml import etree
import hashlib
from bs4 import BeautifulSoup
def check_for_duplicates(url,content,page_content):
    content_save = etree.tostring(etree.HTML(content), pretty_print = True, method = "html")
    soup = BeautifulSoup(content_save, features="lxml") # scraping through BeautifulSoup to get visible text
    for script in soup(['style', 'script', '[document]', 'head', 'title', 'paragraph', 'p']):
        script.extract()
    visible_text = soup.getText().replace("\n","").replace(" ","").replace("/","")
    re.sub(r'[^\w]', '',visible_text)
    source = hashlib.md5(str(visible_text).encode('utf-8')).hexdigest() # hashing library
    if source in page_content:
        return []
    else: 
        page_content[source] = url
