# parser.py
from bs4 import BeautifulSoup, Comment
import re

def clean_html(html_content):
    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove <script> tags except those containing JSON-LD schema data
    for tag in soup.find_all('script'):
        if tag.get('type') != 'application/ld+json':
            tag.decompose()

    # Remove <style> tags and their contents
    for style_tag in soup.find_all('style'):
        style_tag.decompose()

    # Remove <nav> tags and their contents
    for nav_tag in soup.find_all('nav'):
        nav_tag.decompose()

    # Remove all tags except specified ones, while unwrapping the others
    tags_to_keep = ['a', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']
    for tag in soup.find_all(True):
        if tag.name not in tags_to_keep:
            tag.unwrap()

    # Remove unwanted attributes from all tags
    attributes_to_remove = ['style', 'class', 'id', 'rel', 'target', 'title']
    for tag in soup.find_all(True):
        for attr in attributes_to_remove + [attr for attr in tag.attrs if attr.startswith("data-") or attr.startswith("on")]:
            if tag.has_attr(attr):
                del tag[attr]

    # Remove comments
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # Remove empty tags (tags with no content or only whitespace)
    for tag in soup.find_all():
        if not tag.contents or (tag.string and not tag.string.strip()):
            tag.decompose()

    # Remove redundant <br> tags with empty or duplicate line breaks
    for br in soup.find_all('br'):
        next_sibling = br.next_sibling
        if not next_sibling or next_sibling.name == 'br' or (next_sibling.string and not next_sibling.string.strip()):
            br.decompose()

    # Convert to string and remove multiple consecutive newlines
    text = str(soup)
    text = re.sub(r'\n\s*\n+', '\n', text)  # Consolidate multiple new lines

    return text
