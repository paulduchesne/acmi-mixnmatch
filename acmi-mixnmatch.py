
import json
import pandas
import pathlib
import pydash
import requests
import tqdm

def value_extract(row, column):

    ''' Extract dictionary values. '''
    
    return pydash.get(row[column], 'value')

def sparql_query(query, service):

    ''' Send sparql request, and formulate results into a dataframe. '''

    response = requests.get(service, params={'format': 'json', 'query': query}, timeout=120)
    results = pydash.get(response.json(), 'results.bindings')
    data_frame = pandas.DataFrame.from_dict(results)
    for column in data_frame.columns:
        data_frame[column] = data_frame.apply(value_extract, column=column, axis=1)
    
    return data_frame

dataframe = pandas.DataFrame(columns=['acmi_id', 'label', 'description'])

# traverse ACMI API to pull film works with appropriate label and description.

api_work_path = pathlib.Path.cwd() / 'acmi-api' / 'app' / 'json' / 'works'
api_works = [x for x in api_work_path.iterdir() if x.suffix == '.json']

for x in tqdm.tqdm(api_works):

    with open(x) as data:
        data = json.load(data)

    if 'type' in data:
        if data['type'] == 'Film' and data['record_type'] == 'work':

            label = data['title']
            for x in ['=', '[', '(']:
                label = label.split(x)[0].strip()

            directors = ', '.join([y['name'] for y in data['creators_primary'] if y['role'] in ['director', 'producer/director']])
            if len(directors):
                directors = 'by '+directors

            date = ''
            if len(data['production_dates']):
                date = data['production_dates'][0]['date']

            desc = f'{date} film {directors}'.strip()
            if len(label):
                dataframe.loc[len(dataframe)] = [(str(data['id'])), (label), (desc)]

# pull entities already matched in wikidata.

query = '''
    select ?wikidata_id ?acmi_id
    where { 
        ?wikidata_id wdt:P7003 ?acmi_id . 
        filter(regex(str(?acmi_id), "works"))
        } '''

wikidata = sparql_query(query, 'https://query.wikidata.org/sparql').drop_duplicates()
wikidata['wikidata_id'] = wikidata['wikidata_id'].str.split('/').str[-1]
wikidata['acmi_id'] = wikidata['acmi_id'].str.split('/').str[-1]
dataframe = pandas.merge(dataframe, wikidata, on='acmi_id', how='left').fillna('')
dataframe.to_csv(pathlib.Path.cwd() / 'acmi-mixnmatch.csv', index=False)
