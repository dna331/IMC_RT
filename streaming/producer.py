from confluent_kafka import Producer
import json
import os
import requests
from bs4 import BeautifulSoup
import sys
import re
from utils import check_entity, check_caption, is_valid_candidate, transform_caption
import uuid
from PIL import Image
from io import BytesIO

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# Configure the Kafka producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'myproducer'
}

# Create a Kafka producer instance
producer = Producer(producer_conf)

# Define the Kafka topic to produce to
topic = 'caption_data'

# Define the message to send
message = {
    'image_url': 'test3.jpg',
    'caption': 'A beautiful sunset123'
}


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

    response = requests.get(search_url, params=search_params, headers=headers)
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
    response = requests.get(page_url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    images = []
    for figure in soup.find_all('figure', class_=['mw-default-size', 'mw-halign-right']):
        img = figure.find('img', src=True, srcset=True)  # Get the srcset attribute
        if img and img.get('srcset'):  # Ensure the img object exists and has a srcset attribute
            # Split the srcset string into a list and get the last URL
            image_url = 'https:' + img['srcset'].split(",")[-1].split()[0]
            image_id = str(uuid.uuid4())  # Generate a unique ID
            caption = figure.find('figcaption')
            caption_text = " ".join(caption.stripped_strings) if caption else None

            images.append({'url': image_url, 'caption': caption_text, 'id': image_id})

    for div in soup.find_all('div', class_=['trow']):
        img = div.find('img', src=True, srcset=True)  # Get the srcset attribute
        if img and img.get('srcset'):  # Ensure the img object exists and has a srcset attribute
            # Split the srcset string into a list and get the last URL
            image_url = 'https:' + img['srcset'].split(",")[-1].split()[0]

            caption = div.find('div', class_='thumbcaption')
            caption_text = " ".join(caption.stripped_strings) if caption else None

            image_id = str(uuid.uuid4())  # Generate a unique ID
            images.append({'url': image_url, 'caption': caption_text, 'id': image_id})

    return images


def download_image(url, format='jpg', quality='original'):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    params = {
        'format': format,
        'quality': quality
    }

    response = requests.get(url, headers=headers, params=params)
    image = Image.open(BytesIO(response.content))

    # Resize the image
    width, height = image.size
    # aspect_ratio = width / height
    # new_width = 650
    # new_height = int(new_width / aspect_ratio)
    # resized_image = image.resize((new_width, new_height))
    #
    # # Save the resized image
    # resized_image.save(file_path)
    return width,height


def sanitize_title(title):
    return re.sub(r'[\\/:"*?<>|]+', '', title)

def main(page_url):
    page_title = page_url.split('/')[-1].replace('_', ' ')
    sanitized_title = sanitize_title(page_title)

    images = get_image_and_caption(page_url)
    download_directory = './Images'

    # if not os.path.exists(download_directory):
    #     os.makedirs(download_directory)

    for idx, image in enumerate(images, start=1):
        file_name = f"{image['id']}.jpg"
        file_path = os.path.join(download_directory, file_name).replace('\\','/')

        width, height = download_image(image['url'], format='png', quality='original')
        print(f"Downloaded image {idx}: {file_path}")

        caption_file_name = f"{sanitized_title}_{idx}_caption.txt"
        caption_file_path = os.path.join(download_directory, caption_file_name)

        if image['caption']:
            print('file_name : ', file_name)
            print('image_url : ', image['url'])
            print('caption : ', image['caption'])

            if check_caption(image['caption']) and is_valid_candidate(image['url'],image['caption']):
                #hypernym transforming caption
                print('filtering phase passed for caption :, ',image['caption'])
                try:
                    image['caption'] = transform_caption(image['caption'])
                except:
                    print(f"the caption : {image['caption']} can not be transformed")
                message = {
                    'image_url': image['url'],
                    'caption': image['caption'],
                    'id' : image['id'],
                    'width' : width,
                    'height' : height
                }
                with open('passed.txt','a',encoding="utf-8") as f1:
                    f1.write(image['url'] + '\t' + image['id'] + '\t' + image['caption'] + '\n')
                # Produce the message to the Kafka topic
                producer.produce(topic, json.dumps(message).encode('utf-8'))

                # Wait for any outstanding messages to be delivered and delivery reports to be received
                producer.flush()

            else:
                print('filtering phase failed for caption :, ', image['caption'])
                with open('violated.txt', 'a', encoding="utf-8") as f:
                    f.write(image['url'] + '\t' + image['caption'] + '\n')


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


