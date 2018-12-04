import json
with open('C:/Users/MMCQUAY/Documents/Reddit Data/RC_2018-01', buffering=1000) as f:
        for row in f:
            ## Increment the row_count and load the json data
            row = json.loads(row)
            print(row)