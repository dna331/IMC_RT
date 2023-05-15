import os
import requests
from bs4 import BeautifulSoup
import sys
import re


def search_wikipedia(query):
    search_url = 'https://vi.wikipedia.org/w/api.php'
    search_params = {
        'action': 'query',
        'list': 'search',
        'format': 'json',
        'srsearch': query,
        'utf8': 1,
        'srlimit': 50  # You can change this number to get more or fewer search results
    }

    response = requests.get(search_url, params=search_params)
    data = response.json()

    if data['query']['search']:
        search_results = [result['title'] for result in data['query']['search']]
        urls = [get_wikipedia_page_url(title) for title in search_results]
        return urls
    else:
        return []


def get_wikipedia_page_url(title):
    base_url = "https://vi.wikipedia.org/wiki/"
    return base_url + title.replace(" ", "_")


def get_image_and_caption(page_url):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    images = []
    for figure in soup.find_all('figure', class_=['mw-default-size', 'mw-halign-right']):
        img = figure.find('img', src=True)
        if img and img.get('src'):  # Ensure the img object exists and has a src attribute
            image_url = 'https:' + img['src']

            caption = figure.find('figcaption')
            caption_text = " ".join(caption.stripped_strings) if caption else None

            images.append({'url': image_url, 'caption': caption_text})

    for div in soup.find_all('div', class_=['trow']):
        img = div.find('img', src=True)
        if img and img.get('src'):  # Ensure the img object exists and has a src attribute
            image_url = 'https:' + img['src']

            caption = div.find('div', class_='thumbcaption')
            caption_text = " ".join(caption.stripped_strings) if caption else None

            images.append({'url': image_url, 'caption': caption_text})

    return images


def download_image(url, file_path):
    response = requests.get(url)
    with open(file_path, 'wb') as file:
        file.write(response.content)


def sanitize_title(title):
    return re.sub(r'[\\/:"*?<>|]+', '', title)


def main(page_url):
    page_title = page_url.split('/')[-1].replace('_', ' ')
    sanitized_title = sanitize_title(page_title)

    images = get_image_and_caption(page_url)
    download_directory = sanitized_title

    # if not os.path.exists(download_directory):
    #     os.makedirs(download_directory)

    for idx, image in enumerate(images, start=1):
        file_name = f"{sanitized_title}_{idx}.jpg"
        # file_path = os.path.join(download_directory, file_name)

        # download_image(image['url'], file_path)
        # print(f"Downloaded image {idx}: {file_path}")

        caption_file_name = f"{sanitized_title}_{idx}_caption.txt"
        caption_file_path = os.path.join(download_directory, caption_file_name)

        if image['caption']:
            print('file_name : ', file_name)
            print('image_url : ', image['url'])
            print('caption : ', image['caption'])
        else:
            print(f"No caption for image {idx}")

        print()


if __name__ == "__main__":
    while True:
        search_term = input("Enter a Wikipedia search term (or 'q' to quit): ")
        if search_term == 'q':
            break

        page_urls = search_wikipedia(search_term)

        if page_urls:
            for page_url in page_urls:
                main(page_url)
        else:
            print("No relevant Wikipedia pages found.")