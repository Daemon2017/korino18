import json
import os
import re

import numpy as np
import pandas as pd
from flask import Response, Flask, request
from waitress import serve

MIME_TYPE = 'application/json'

app = Flask(__name__)
common_df = pd.DataFrame()


def prepare_data():
    global common_df
    dir_name = "ready"
    file_name_regex = r"(\w*)_(\d*)_(\d*).csv"
    files = os.listdir(dir_name)
    csv_files = [file for file in files if file.endswith('.csv')]
    common_df = pd.DataFrame()
    for csv_file in csv_files:
        print("Обрабатывается файл {0}/{1}".format(dir_name, csv_file))
        df = pd.read_csv(dir_name + "/" + csv_file)
        is_exists_duplicates_in_file(df, csv_file)
        result = re.search(file_name_regex, csv_file)
        df['Год'] = result.group(2)
        common_df = pd.concat([common_df, df], sort=False)
    common_df = common_df.replace(np.nan, None)
    common_df["Номер личный"] = common_df["Номер личный"].astype(int, errors='ignore')
    common_df["Номер отца"] = common_df["Номер отца"].astype(int, errors='ignore')
    common_df["Возраст ныне"] = common_df["Возраст ныне"].astype(float, errors='ignore')
    common_df["Год"] = common_df["Год"].astype(int, errors='ignore')
    print("Подготовка данных завершена!")


def is_exists_duplicates_in_file(df, csv_file):
    duplicates = df[df['Номер личный'].duplicated(keep=False)]
    if duplicates.size > 0:
        print("В файле {0} выявлены неуникальные значения в столбце \"Номер личный\", дальнейшая работа невозможна."
              .format(csv_file))
        print(duplicates)
        exit()


@app.route('/find_person', methods=['GET'])
def find_person():
    num_own = request.args.get('num_own')
    num_father = request.args.get('num_father')
    name_church = request.args.get('name_church')
    patronym_church = request.args.get('patronym_church')
    name_pagan = request.args.get('name_pagan')
    patronym_pagan = request.args.get('patronym_pagan')
    age = request.args.get('age')
    year = request.args.get('year')

    rows = common_df
    if num_own is not None:
        rows = rows[rows['Номер личный'].astype(str).str.contains(str(num_own))]
    if num_father is not None:
        rows = rows[rows['Номер отца'].astype(str).str.contains(str(num_father))]
    if name_church is not None:
        rows = rows[rows['Имя церковное'].astype(str).str.contains(str(name_church))]
    if patronym_church is not None:
        rows = rows[rows['Отчество церковное'].astype(str).str.contains(str(patronym_church))]
    if name_pagan is not None:
        rows = rows[rows['Имя мирское'].astype(str).str.contains(str(name_pagan))]
    if patronym_pagan is not None:
        rows = rows[rows['Отчество мирское'].astype(str).str.contains(str(patronym_pagan))]
    if age is not None:
        rows = rows[rows['Возраст ныне'].astype(str).str.contains(str(age))]
    if year is not None:
        rows = rows[rows['Год'].astype(str).str.contains(str(year))]
    rows_dicts = rows.to_dict('records')
    rows_dicts = [{k: v for k, v in rows_dict.items() if v} for rows_dict in rows_dicts]
    response = json.dumps(rows_dicts, ensure_ascii=False)
    return Response(response, mimetype=MIME_TYPE)


@app.route('/get_ancestors_tree', methods=['GET'])
def get_ancestors_tree():
    ancestor_number = request.args.get('ancestor_number')

    is_ancestor_exists = True
    tree = {}
    i = 0
    while is_ancestor_exists:
        sorted_rows = common_df.loc[common_df['Номер личный'] == int(ancestor_number)].sort_values(by=['Год'],
                                                                                                   ascending=False)
        dict_record = sorted_rows.to_dict('records')[0]

        notnull_father_rows = sorted_rows[sorted_rows['Номер отца'].notnull()]
        if len(notnull_father_rows.index) != 0:
            dict_record["Номер отца"] = notnull_father_rows['Номер отца'].iloc[0]
            ancestor_number = notnull_father_rows['Номер отца'].iloc[0]
        else:
            is_ancestor_exists = False

        notnull_age_rows = sorted_rows[sorted_rows['Возраст ныне'].notnull()]
        if len(notnull_age_rows) != 0:
            dict_record["Год рождения"] = notnull_father_rows['Год'].iloc[0] - \
                                          notnull_father_rows['Возраст ныне'].iloc[0]

        if i != 0:
            dict_record["Ребенок"] = tree
        dict_record.pop('Возраст ныне', None)
        dict_record.pop('Год', None)
        dict_record = {k: v for k, v in dict_record.items() if v}
        tree = dict_record
        i = i + 1
    response = json.dumps(tree, ensure_ascii=False)
    return Response(response, mimetype=MIME_TYPE)


if __name__ == '__main__':
    prepare_data()

    serve(app, host="0.0.0.0", port=8080)
