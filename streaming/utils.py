# Text filtering function using spacy in vietnamese
import spacy
from google.cloud import translate_v2 as translate
import google.oauth2
from google.cloud import vision_v1
from nltk.stem import PorterStemmer
import requests
import urllib
import json
import pandas as pd
from requests_html import HTML
from requests_html import HTMLSession
from io import BytesIO
from google.oauth2 import service_account
from google.cloud import language_v1
from google.cloud.language_v1 import Document, EncodingType
from collections import defaultdict
import re
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'D:\Kafka_Streaming\streaming\wikicrawling-361e2822c0a0.json'
nlp = spacy.load("vi_core_news_lg") # Load the pre-trained Vietnamese model

#text filtering
def check_caption(caption):
    # Define the set of allowed POS tags in Vietnamese
    allowed_tags = {'N', 'Np', 'V', 'A', 'R'}

    # Define the set of forbidden substrings
    forbidden_substrings = {'click to enlarge', 'stock photo', 'embedded image permalink', 'profile photo'}

    # Tokenize the caption into individual words using spaCy
    words = [token.text for token in nlp(caption)]

    # Filter out stop words and punctuation
    words = [w for w in words if not nlp.vocab[w].is_punct]

    # Compute the ratio of unique words to total words
    unique_ratio = len(set(words)) / len(words)

    # Tag the parts of speech for each word
    tagged_words = [(token.text, token.tag_) for token in nlp(caption)]
    print('tagged_words : ',tagged_words)

    # Compute the ratio of allowed POS tags to total tags
    allowed_ratio = sum(1 for w, t in tagged_words if t in allowed_tags) / len(tagged_words)
    print('allowed_ratio : ',allowed_ratio)

    # Compute the ratio of nouns and proper nouns to total words
    noun_ratio = sum(1 for w, t in tagged_words if t in ['N', 'Np']) / len(words)
    print('noun_ratio : ',noun_ratio)

    # Compute the ratio of repeated tokens to total words
    repeat_ratio = (len(words) - len(set(words))) / len(words)
    print('repeat_ratio : ',repeat_ratio)

    # Compute the ratio of capitalized words to total words
    cap_ratio = sum(1 for w in words if w[0].isupper()) / len(words)
    print('cap_ratio : ',cap_ratio)

    # Check if the caption contains any forbidden substrings
    contains_forbidden = any(s in caption.lower() for s in forbidden_substrings)

    # Print the violated rule number
    if unique_ratio <= 0.5:
        print("Rule 1 is violated: unique word ratio is too low")
    if allowed_ratio <= 0.3:
        print("Rule 2 is violated: allowed POS tag ratio is too low")
    if noun_ratio >= 0.9:
        print("Rule 3 is violated: noun ratio is too high")
    if repeat_ratio >= 0.5:
        print("Rule 4 is violated: repeated token ratio is too high")
    if cap_ratio >= 0.75:
        print("Rule 5 is violated: capitalized word ratio is too high")
    if contains_forbidden:
        print("Rule 6 is violated: caption contains a forbidden substring")

    # Return True if all conditions are met, False otherwise
    return (unique_ratio > 0.5 and
            allowed_ratio > 0.3 and
            noun_ratio < 0.9 and
            repeat_ratio < 0.5 and
            cap_ratio < 0.75 and
            not contains_forbidden)


#Translate function
def translate_to_vietnamese(text):
    client = translate.Client()
    result = client.translate(text, target_language='vi')
    return result['translatedText']


#image-caption pairs filtering
def is_valid_candidate(image_url, caption):
    # Load image and extract labels using Google Cloud Vision API
    vision_client = vision_v1.ImageAnnotatorClient()
    image_context = vision_v1.ImageContext()
    # Initialize Porter stemmer
    stemmer = PorterStemmer()
    response = requests.get(image_url)
    image = vision_v1.types.Image(content=BytesIO(response.content).read())
    response = vision_client.label_detection(image=image, image_context={'language_hints': ['en']})
    # print(response)
    image_labels = set([label.description.lower() for label in response.label_annotations])

    # Translate english label to vietnamese
    vn_labels = [translate_to_vietnamese(label) for label in image_labels]
    print(vn_labels)
    # Tokenize and stem caption text
    caption_tokens = [stemmer.stem(token.text.lower()).replace('_',' ') for token in nlp(caption)]
    print(caption_tokens)
    # Check if any caption tokens are valid and match image labels
    overlap = False
    for token in caption_tokens:
        if any(vn_label == token for vn_label in vn_labels):
            overlap = True
            break

    return overlap


def get_source(url):

    try:
        session = HTMLSession()
        response = session.get(url)
        return response

    except requests.exceptions.RequestException as e:
        print(e)


# knowledge graph function
def knowledge_graph(api_key, query):
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    params = {
        'query': query,
        'limit': 1,
        'indent': True,
        'key': api_key,
        'languages': 'vi'
    }

    url = service_url + '?' + urllib.parse.urlencode(params)
    response = get_source(url)

    return json.loads(response.text)


# check name entity of word
def check_entity(text):
    # Instantiates a client
    client = language_v1.LanguageServiceClient()

    # The text to analyze
    # text = u'Justin Bieber'

    # The language of the text
    document = Document(content=text, type_=Document.Type.PLAIN_TEXT, language="en")

    # Encoding type for the text
    encoding_type = EncodingType.UTF8

    # Analyze the text for named entities
    response = client.analyze_entities(request={'document': document, 'encoding_type': encoding_type})

    # Print the entities and their types
    print(response.entities[0].name, response.entities[0].type_)
    return response.entities[0].type_


def remove_date(text):
    # Define the Vietnamese date patterns
    date_patterns = [
       r"\b\w+\b\sngày\s\d{1,2}\s(tháng)\s\d{1,4}\s(năm)\s\d{1,4}\b",  # ngày dd tháng mm (năm) yyyy
        r'\b\w+\b\sngày\s\d{1,2}\s(tháng)\s\d{1,4}\b',                   # ngày dd tháng mm
        r'\b\w+\b\sngày\s\d{1,2}\b',                                      # ngày dd
        r'\b\w+\b\s(năm)\s\d{1,4}\b',                                     # n năm yyyy
        r'\b\w+\b\s(tháng)\s\d{1,4}\b',                                    # tháng mm năm yyyy
        r'\b\w+\b\s(tháng)\s\d{1,4}\s(năm)\s\d{1,4}\b',
        r'\b\w+\b\s\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b',  # dd/mm/yy or dd/mm/yyyy
        r'\b\w+\b\s\d{1,2}[-/]\d{1,2}\b'
    ]

    # Combine all patterns into a single regular expression
    combined_pattern = '|'.join(date_patterns)

    # Remove the date information from the text
    cleaned_text = re.sub(combined_pattern, '', text)

    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)  # Remove extra whitespace

    return cleaned_text.strip()

#hypernymed transformation
def transform_caption(caption):
    # Load the pre-trained Vietnamese model
    nlp = spacy.load("vi_core_news_lg")

    # Define the set of POS tags for allowed nouns and adjectives
    allowed_tags = {"N", "Np", "A"}

    # Tokenize the caption using spaCy
    doc = nlp(caption)

    # Create a dictionary to keep track of noun phrases with the same head
    noun_phrases = defaultdict(list)

    # Create a list to store the modified tokens
    modified_tokens = []
    api_key = "add api key here"

    # Loop through the tokens in the caption
    for token in doc:
        # Remove noun modifiers of certain types
        # if token.tag_ in allowed_tags and token.dep_ in {"compound", "nummod", "amod"}:
        #     token.head = token.head.head

        # Match named entities against the KG entries and substitute with their hypernym
        if token.tag_ == 'Np' and check_entity(token.text.replace('_', ' ')) != language_v1.Entity.Type.LOCATION:
            entity_text = token.text
            try:
                entity_hypernym = knowledge_graph(api_key, entity_text.replace('_', ' '))['itemListElement'][0]['result'][
                    'description']
            except:
                continue

            if entity_hypernym:
                # modified_token = token
                # modified_token.text = entity_hypernym
                modified_tokens.append(entity_hypernym)
                continue
        if token.tag_ == 'Np' and check_entity(token.text.replace('_', ' ')) == language_v1.Entity.Type.LOCATION:
            continue
        # Resolve coordination noun-phrases with the same head
        # if token.dep_ == "conj" and token.head.tag_ in allowed_tags:
        #     head = token.head
        #     noun_phrases[head].append(token)

        # Append the token to the modified_tokens list
        modified_tokens.append(token.text)

    # Pluralize resulting coordination noun-phrases with the same head
    # print(noun_phrases)
    # for head, tokens in noun_phrases.items():
    #     if len(tokens) > 0:
    #         head_text = head.text.lower()
    #         head_plural = 'Các ' + head_text
    #         modified_tokens.append(head_plural)

    # Construct the transformed caption using the modified_tokens list
    transformed_caption = " ".join(modified_tokens)

    # Return the transformed caption
    return remove_date(transformed_caption.replace('_',' '))