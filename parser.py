# parser.py
from bs4 import BeautifulSoup

def clean_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove unwanted tags
    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript', 'iframe']):
        tag.decompose()

    # Extract text
    text = ' '.join(soup.stripped_strings)
    return text
