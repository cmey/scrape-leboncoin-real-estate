import csv
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading

import requests
from bs4 import BeautifulSoup


URL_template = 'https://www.leboncoin.fr/locations/offres/ile_de_france/occasions/?o={}'
max_page_number = 2000

OUTPUT_CSV_FILE = 'rent.csv'
HEADER = ['price', 'city', 'departement']

# Serialize writing to the CSV file.
# Multiple scape tasks put lines to write to the queue,
# a single writer task gets lines from the queue and saves to CSV file.
queue = Queue()


def entries_for_page(page_number):
    URL = URL_template.format(page_number)
    r = requests.get(URL)
    soup = BeautifulSoup(r.content.decode('iso-8859-1').encode('utf8'), 'lxml')

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


def get_rent_infos(page_number):
    page_entries = entries_for_page(page_number)
    for page_entry in page_entries:
        try:
            price = extract_price(page_entry)
            address = extract_address(page_entry)
            yield dict(price=price, address=address)
        except:
            pass


def scrape_task(page_number):
    for rent_info in get_rent_infos(page_number):
        price = rent_info['price']
        address = rent_info['address']
        row = [price, address['city'], address['departement']]
        queue.put(row)


def write_task():
    def do_work(file_writer, file, row):
        print('writing', row)
        file_writer.writerow(row)
        file.flush()

    with open(OUTPUT_CSV_FILE, 'w') as file:
        file_writer = csv.writer(file)
        do_work(file_writer, file, HEADER)
        while True:
            item = queue.get()
            if item is None:
                queue.task_done()
                break
            do_work(file_writer, file, item)
            queue.task_done()


def main():
    write_thread = threading.Thread(target=write_task)
    write_thread.start()

    num_worker_threads = 8
    with ThreadPoolExecutor(num_worker_threads) as pool:
        page_numbers = range(1, max_page_number)
        pool.map(scrape_task, page_numbers)

    queue.put(None)  # poison pill to end write_task


if __name__ == '__main__':
    main()
