import pandas as pd
import glob
import os
import pickle
import pymongo
from tqdm import tqdm
if __name__ == "__main__":
    f_list = sorted(glob.glob('/Users/LeeYK/Documents/jupyters/PythonScripts/input_data/*.txt'))
    zinc_list = set()
    for f in f_list:
        out_dict = {}
        fname = os.path.basename(f).split('.')[0]
        df = pd.read_csv(f,sep='\t',header=None)
        df = df[df[2]!=0]

        for idx, row in df.iterrows():
            for zinc_id in row[2].split(','):
                zinc_list.add(zinc_id)
                if zinc_id in out_dict:
                    temp = out_dict[zinc_id][fname]
                else:
                    temp = []
                temp.append(row[0])
                out_dict[zinc_id] = {fname:temp}
        with open(f'/Users/LeeYK/Documents/jupyters/PythonScripts/output_data/{fname}.pkl','wb') as f:
            pickle.dump(out_dict,f)
    pickle_list = sorted(glob.glob('/Users/LeeYK/Documents/jupyters/PythonScripts/output_data/*.pkl'))
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['zinc_db']
    collection = db['zinc_collection']
    for zid in tqdm(zinc_list):
        final_dict = {}
        for p in pickle_list:
            fname = os.path.basename(p).split('.')[0]
            with open(p,'rb') as f:
                temp = pickle.load(f)
            if zid in temp:
                final_dict[zid] = temp[zid]
        # mongodb insert
        collection.insert_one(final_dict)
        client.close()
            
    