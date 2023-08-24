from flask_restful import Api, Resource, reqparse
from flask import Flask, jsonify, make_response
import json, time, io
import pandas as pd
import numpy as np
from tqdm import tqdm
from pathlib import Path

app = Flask(__name__)
api = Api(app)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True


def load_datasets(folderpath):
    """
    load data from all csv files in a folder and optimize columns for fast querying
    folderpath should be a Path() object
    """
    dataframes = {}
    for file in folderpath.glob("*.csv"):
        print("")
        print(f"Loading {file.name}..")
        starttime = time.time()
        df = pd.read_csv(file, low_memory=False)
        print(f"Dataframe load time: {np.round(time.time() - starttime, 2)} sec")
        buffer = io.StringIO()
        df.info(buf=buffer, verbose=False)
        info = buffer.getvalue()
        info = info.split('\n')
        print(f"{info[1].split(',')[0]}, {info[-2]}")
        print("")
        print("Optimizing dataset..")
        objcols = []
        for c in df.columns:
            if df[c].dtype == 'O':
                objcols.append(c)
        for c in tqdm(objcols):
            df[c] = df[c].astype('category')
        print("Done. Optimized data:")
        df.info(buf=buffer, verbose=False)
        info = buffer.getvalue()
        info = info.split('\n')
        print(f"{info[1].split(',')[0]}, {info[-2]}")
        print("")
        dataframes[file.stem] = df  # use filename without extension as key
    return dataframes


def get_top_uniques(x, k=1):
    '''
    finds unique values for a provided list/array of values
    creates counts for each value and sorts in descending order
    calculates statistics
    returns top K
    '''
    arr_len = len(x)
    # get uniques
    values, counts = np.unique(x, return_counts=True)
    # get % of occurences
    rel_counts = np.round(counts / arr_len, 3)
    # put in a dictionary
    uniques = dict(zip(values, rel_counts))
    sorted_tuples = sorted(uniques.items(), key=lambda item: item[1], reverse=True)
    sorted_uniques = {k: v for k, v in sorted_tuples}
    result = dict(list(sorted_uniques.items())[0: k])
    return result


def make_query(q_args):
    # create query
    q_str = ''
    for i in q_args.items():
        q_str += f'({i[0]} == "{i[1]}") and '
    return q_str[:-5]


# specify the folder path
folder_path = Path('load')

# load all datasets
dfs = load_datasets(folder_path)


class ClaimProf(Resource):

    def get(self, input):
        # time
        starttime = time.time()

        # read input
        try:
            q_args = dict(json.loads(input))
        except:
            return 'Please provide valid input in {"Parameter":"Value", ...} format', 404

        # create hierarchy of fields
        hierarchy = ['PricingActionCode',
                     'ProcedureCode',
                     'DetailPaidAmount',
                     'DetailAllowedAmount',
                     'ProcedureCodeModifier1', 'ProcedureCodeModifier2', 'ProcedureCodeModifier3',
                     'ProcedureCodeModifier4',
                     'PlaceofService',
                     'DetailBilledAmount',
                     'HeaderDiagnosesPointersAssociatedwithdetailCSV',
                     'DiagnosisCode1', 'DiagnosisCode2', 'DiagnosisCode3', 'DiagnosisCode4', 'DiagnosisCode5',
                     'DiagnosisCode6', 'DiagnosisCode7', 'DiagnosisCode8', 'DiagnosisCode9']

        # storing non empty fields while maintaining hierarchy
        hi_non_emp = [field for field in hierarchy if field in q_args.keys()]

        res_dfs = []
        for df_name, df in dfs.items():
            # check if parameters exist in columns list
            for arg in q_args:
                if arg not in df.columns:
                    return f"Uknown parameter {arg}. Available fields are: {df.columns.to_list()}", 404

            # create a subset, check if empty
            q = df.query(make_query(q_args))
            removed_args = []
            if q.shape[0] == 0:
                print('initial query did not return result')
                while q.shape[0] == 0 and len(q_args) > 0:
                    print(f'removing {hi_non_emp[-1]}')
                    del q_args[hi_non_emp[-1]]
                    removed_args.append(hi_non_emp[-1])
                    del hi_non_emp[-1]
                    q = df.query(make_query(q_args))

            # find uniques and calculate the most frequent ones
            res_df = pd.DataFrame(columns=q.columns, index=np.arange(1))
            for i in tqdm(q.columns):
                res_df[i] = get_top_uniques(q[i]).keys()
            res_df['removed_args'] = [removed_args]
            res_df['source_file'] = [df_name]

            res_dfs.append(res_df)

        print(f"Request time: {np.round(time.time() - starttime, 2)} sec")

        # concatenate all result dataframes
        result = pd.concat(res_dfs, ignore_index=True)

        res = make_response(jsonify(result.to_json(orient="records")), 200)
        return res


api.add_resource(ClaimProf, "/claims/prof", "/claims/prof/", "/claims/prof/<string:input>")

if __name__ == '__main__':
    app.run(debug=True)
