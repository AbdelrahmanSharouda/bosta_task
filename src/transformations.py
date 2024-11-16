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

with open(r'C:\Users\magdy\boodi\origin\data\raw\raw_json.json') as file:
    data = json.load(file)

jdata = [flatten(line) for line in data]   
df = pd.DataFrame(jdata)
print(df.info())


# Too many columns with missing values.
# Extra step to clean the columns.
threshold = 0.95  # Adjust this value based on your needs (95% missing values)
min_count = int(len(df) * (1 - threshold)) 
df = df.dropna(axis=1, thresh=min_count)

# Could also be concatenated by a comma-delimeter 

# Optional: Fill remaining NA values
# df = df.fillna('')  # or df.fillna(0) for numeric columns
print(df.info())

# df.to_csv(r'C:\Users\magdy\boodi\origin\data\flattened\flattened.csv')