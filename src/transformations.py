import pandas as pd
import json

def flatten(doc, parent_key='', sep='_'):
    items = []
    for k,v in doc.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, sub_item in enumerate(v):
                if isinstance(sub_item,dict):
                    items.extend(flatten(sub_item, f'{new_key}{sep}{i}', sep=sep).items())
                else:
                    items.append((f"{new_key}{sep}{i}", sub_item))
        else:
            items.append((new_key,v))
    return dict(items)


with open(r'D:\python_files\Bosta\data\raw\raw.json') as file:
    data = json.load(file)

jdata = flatten(data)
df = pd.DataFrame.from_dict(jdata, orient='index', columns=['value'])
df.to_csv(r'D:\python_files\Bosta\data\flattened\flattened.csv')