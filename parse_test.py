import sys
import codecs
from bs4 import BeautifulSoup
import json
import re

file_path = "ATBB検索結果一覧.txt"
html_content = ""
with codecs.open(file_path, 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

# In ATBB search results, a property card usually has a specific structure.
# Let's find rows or wrappers that contain 'property' or 'bukken' classes, or maybe the main tables.
# Often each property is a 'tbody' if it is a table layout, or a div wrapper.
# Looking at the original script find_property_cards used:
# "button[name='shosai'], button[id^='shosai']" -> then finding ancestor matching class 'property', 'bukken', 'card', 'item'
# OR CSS ".property_card, [class*='property'], [class*='bukken']"

cards = soup.select(".property_card, [class*='property'], [class*='bukken'], tr.item, tbody.item")
if len(cards) == 0:
    # Just grab anything with 'name' or 'bukkenno' or whatever identifies a property
    cards = soup.select("tbody") # sometimes each property is a tbody

print(f"Found {len(cards)} potential property elements.")

extracted_data = []

# Let's write a robust extraction function
def extract_text_by_label(card, label_pattern):
    # Find head/th/etc that contains the label
    label_elem = card.find(string=re.compile(label_pattern))
    if label_elem:
        # Usually the value is in the next cell / element
        parent = label_elem.find_parent(['th', 'td', 'dt'])
        if parent:
            next_sibling = parent.find_next_sibling(['td', 'dd'])
            if next_sibling:
                # remove any inner script/style
                for el in next_sibling(['script', 'style']):
                    el.decompose()
                text = next_sibling.get_text(separator=' ', strip=True)
                # remove double spaces
                return re.sub(r'\s+', ' ', text)
    return None

def extract_rent(card):
    # Rent is notoriously an image in ATBB. But there might be alt text.
    rent_label = card.find(string=re.compile(r"賃料"))
    if rent_label:
        parent = rent_label.find_parent(['th', 'td', 'dt', 'span', 'p'])
        if parent:
            next_sibling = parent.find_next_sibling(['td', 'dd', 'span', 'p'])
            if next_sibling:
                img = next_sibling.find('img')
                if img and img.get('alt'):
                    return img.get('alt')
                return next_sibling.get_text(separator=' ', strip=True)
    
    # Try finding an image with id starting with price_img
    imgs = card.select("img[id^='price_img']")
    if imgs and imgs[0].get('alt'):
        return imgs[0].get('alt')
        
    # Regex fallback
    text = card.get_text(separator=' ')
    match = re.search(r"賃料\s*([\d,\.]+万円?)", text)
    if match: return match.group(1)
    
    return "Unknown"

# Since ATBB list items might be flat or nested, let's explore the text of the first one that has "万円" or "セレスティア"
property_card = None
for card in cards:
    text = card.get_text(separator=' ')
    if "セレスティア" in text or "万円" in text:
        property_card = card
        break

if not property_card:
    print("Could not find a valid property card with dummy data search.")
    if len(cards) > 0: property_card = cards[5] # just pick one

if property_card:
    # Let's do a raw text dump to see exactly what we're dealing with
    raw_text = property_card.get_text(separator='\n', strip=True)
    print("--- RAW TEXT OF FIRST CARD ---")
    print(raw_text[:1000]) # Print first 1000 chars

    # Let's try heuristic extraction just by splitting text
    lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
    print("--- LINES ---")
    for i, line in enumerate(lines[:30]):
        print(f"{i}: {line}")

