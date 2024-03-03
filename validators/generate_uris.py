import re
import json
import uuid

def mineral_site_uri(site):
    try:
        if site is None:
            raise
        processed_data = process_mineral_site(site)
        return ({"result": processed_data})
    except Exception as e:
        return ({"error": str(e)})

def deposit_type_uri(data):
    try:
        if data is None:
            raise
        processed_data = process_deposit_type(data)
        return ({"result": processed_data})
    except Exception as e:
        return ({"error": str(e)})


def mineral_system_uri(data):
    try:
        if data is None:
            raise
        processed_data = process_mineral_system(data)
        return ({"result": processed_data})
    except Exception as e:
        return ({"error": str(e)})


def document_uri(data):
    try:
        json_param = data.get('document')
        if json_param is None:
            raise

        processed_data = process_document(json_param)

        return ({"result": processed_data})

    except Exception as e:
        return ({"error": str(e)})

def mineral_inventory_uri(data):
    try:

        param1 = data.get('site')
        param2 = data.get('id')
        # Check if both parameters are provided
        if param1 is None or param2 is None:
            raise

        processed_data = process_mineral_inventory(param1, param2)
        return ({"result": processed_data})

    except Exception as e:
        return ({"error": str(e)})

def process_mineral_site(ms):
    merged_string=''

    if 'source_id' in ms and 'record_id' in ms:
        merged_string = (f"{ms['source_id']}-{str(ms['record_id'])}")
        merged_string = slugify(merged_string)
    elif 'source_id' in ms:
        merged_string = (f"{ms['source_id']}")
        merged_string = slugify(merged_string)
    elif 'record_id' in ms:
        merged_string = (f"{ms['record_id']}")
        merged_string = slugify(merged_string)
    else:
        return str(uuid.uuid4())

    if merged_string == '':
        return str(uuid.uuid4())
    return merged_string



def process_mineral_system(ms):
    merged_string = ''

    fields = ['source', 'pathway', 'trap', 'preservation', 'energy', 'outflow']

    for f in fields:

        if f in ms:
            f_object = ms[f]
            if 'theoretical' in f_object:
                merged_string += slugify(f_object['theoretical'])
            if 'criteria' in f_object:
                merged_string += slugify(f_object['criteria'])

    if 'deposit_type' in ms:
        for dt in ms['deposit_type']:
            merged_string += slugify(dt)


    if merged_string == '':
        return ""
    return merged_string


def process_deposit_type(data):
    merged_string = ''
    if 'observed_name' in data:
        merged_string = merged_string + slugify(data['observed_name'])
    merged_string += '-'

    if 'source' in data:
        merged_string = merged_string + slugify(data['source'])
    merged_string += '-'

    if 'normalized_uri' in data:
        merged_string = merged_string+ slugify(data['normalized_uri'])
    merged_string += '-'

    if 'confidence' in data:
        merged_string = merged_string + slugify(str(data['confidence']))
    merged_string += '-'

    if merged_string == '':
        return str(uuid.uuid4())

    return merged_string

def process_document(data):
    merged_string = ''
    if 'doi' in data:
        merged_string = merged_string + slugify(data['doi'])
    merged_string += '-'

    if 'uri' in data:
        merged_string = merged_string + slugify(data['uri'])
    merged_string += '-'

    if 'title' in data:
        merged_string = merged_string+ slugify(data['title'])
    merged_string += '-'

    if 'year' in data:
        merged_string = merged_string + slugify(str(data['year']))
    merged_string += '-'

    if 'authors' in data:
        merged_string = merged_string  + slugify(str(data['authors']))
    merged_string += '-'

    if 'month' in data:
        merged_string = merged_string + slugify(str(data['month']))
    merged_string += '-'

    if merged_string == '':
        return str(uuid.uuid4())

    return merged_string

def process_mineral_inventory(ms, id):
    merged_string = ''

    uri_ms = process_mineral_site(ms)

    if 'MineralInventory' in ms:
        list_mi = ms['MineralInventory']
        process_mi = list_mi[int(id)]
        reference = process_mi['reference']
        document = reference['document']
        uri_doc = process_document(document)
        commodity = process_mi['commodity']
        category_str = ','.join(process_mi.get('category', []))

        merged_string += (uri_ms + '-' + uri_doc + '-' + slugify(commodity) + '-' + slugify(category_str))


    if merged_string == '':
        return ""
    return merged_string


def slugify(s):
    ''' Simplifies ugly strings into something URL-friendly.
    slugify("[Some] _ Article's Title--"): some-articles-title. '''

    s = s.lower()
    for c in [' ', '-', '.', '/']:
        s = s.replace(c, '_')
    s = re.sub('\W', '', s)
    s = s.replace('_', ' ')
    s = re.sub('\s+', ' ', s)
    s = s.strip()
    s = s.replace(' ', '')
    return s


