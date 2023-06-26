import pandas as pd
import glob
import os
import pickle
import pymongo
from tqdm import tqdm


def make_id_dict(f_list):
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
    return zinc_list


def make_mongo_dict(pickle_list,zinc_list):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['zinc_db']
    collection = db['zinc_collection']
    for zid in tqdm(list(zinc_list)[:100]):
        final_dict = {zid:{}}
        for p in pickle_list:
            fname = os.path.basename(p).split('.')[0]
            with open(p,'rb') as f:
                temp = pickle.load(f)
            if zid in temp:
                t_dict = temp[zid]
                final_dict[zid].update(t_dict)
        # mongodb insert
        collection.insert_one(final_dict)
    client.close()
    print('Done')


def main():
    f_list = sorted(glob.glob('/Users/LeeYK/Documents/jupyters/PythonScripts/input_data/*.txt'))[:5]
    zinc_list = make_id_dict(f_list)
    pickle_list = sorted(glob.glob('/Users/LeeYK/Documents/jupyters/PythonScripts/output_data/*.pkl'))
    make_mongo_dict(pickle_list,zinc_list)



if __name__ == "__main__":
    main()
    