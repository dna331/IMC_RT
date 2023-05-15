import json

with open('uitviic_captions_train2017.json', encoding='utf-8') as f:
    data = json.load(f)

image_ids = [image['id'] for image in data['images']]
is_new_list = [{'image_id': image_id, 'is_new': False} for image_id in image_ids]
data['is_new'] = is_new_list

with open('uitviic_captions_train2017_updated.json', 'w') as f:
    json.dump(data, f, indent=4)