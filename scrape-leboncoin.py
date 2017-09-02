import csv

import requests
from bs4 import BeautifulSoup


URL_template = 'https://www.leboncoin.fr/locations/offres/ile_de_france/occasions/?o={}'
max_page_number = 2000


def entries_for_page(page_number):
    URL = URL_template.format(page_number)
    r = requests.get(URL)
    soup = BeautifulSoup(r.content.decode('utf-8', 'ignore'), 'lxml')

    page_entries = soup.find_all('section', {'class': 'item_infos'})
    return page_entries


def extract_price(page_entry):
    price_str_dirty = page_entry.find_all('h3', {'class': 'item_price'})[0].text
    price_digits_list = [s for s in price_str_dirty.split() if s.isdigit()]
    price_str = ''.join(price_digits_list)
    return int(price_str)


def extract_address(page_entry):
    address_pieces_items = page_entry.find_all('meta', {'itemprop': 'address'})
    address_pieces = list(map(lambda p: p.attrs['content'], address_pieces_items))
    address = dict(city=address_pieces[0], departement=address_pieces[1])
    return address


def get_rent_infos():
    for page_number in range(1, max_page_number):

        page_entries = entries_for_page(page_number)
        for page_entry in page_entries:
            try:
                price = extract_price(page_entry)
                address = extract_address(page_entry)
                yield dict(price=price, address=address)
            except:
                pass


def main():
    with open('rent.csv', 'w') as file:
        file_writer = csv.writer(file)
        file_writer.writerow(['price', 'city', 'departement'])

        for rent_info in get_rent_infos():
            price = rent_info['price']
            address = rent_info['address']
            print(price, address)
            file_writer.writerow([price, address['city'], address['departement']])

if __name__ == '__main__':
    main()
