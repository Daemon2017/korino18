import json
import os
import re

import numpy as np
import pandas as pd
from flask import Response, Flask, request
from waitress import serve

MIME_TYPE_JSON = 'application/json'
MIME_TYPE_CSV = 'text/csv'

app = Flask(__name__)
common_df = pd.DataFrame()


def prepare_data():
    global common_df
    dir_name = "ready"
    directories = [dir for dir in os.listdir(dir_name) if os.path.isdir(os.path.join(dir_name, dir))]
    for directory in directories:
        files = os.listdir(os.path.join(dir_name, directory))
        csv_files = [file for file in files if file.endswith('.csv')]
        if len(csv_files) > 0:
            file_name_regex = r"(\w*)_(\d*)_(\d*).csv"
            current_df = pd.DataFrame()
            for csv_file in csv_files:
                print("Обрабатывается файл {0}".format(os.path.join(dir_name, directory, csv_file)))
                df = pd.read_csv(os.path.join(dir_name, directory, csv_file))
                is_exists_duplicates_in_directory(df, csv_file)
                result = re.search(file_name_regex, csv_file)
                df['Год'] = result.group(2)
                current_df = pd.concat([current_df, df], sort=False)
            is_exists_duplicates_in_directory(current_df, directory)
            common_df = pd.concat([common_df, current_df], sort=False)
    common_df = common_df.replace(np.nan, None)
    common_df["Номер личный"] = common_df["Номер личный"].astype(int, errors='ignore')
    common_df["Номер отца"] = common_df["Номер отца"].astype(int, errors='ignore')
    common_df["Возраст ныне"] = common_df["Возраст ныне"].astype(float, errors='ignore')
    common_df["Год"] = common_df["Год"].astype(int, errors='ignore')
    print("Подготовка данных завершена!")


def is_exists_duplicates_in_directory(df, directory):
    duplicates = df[df['Номер личный'].duplicated(keep=False)]
    if duplicates.size > 0:
        print("В папке/файле {0} выявлены неуникальные значения "
              "в столбце \"Номер личный\", "
              "дальнейшая работа невозможна.".format(directory))
        print(duplicates)
        exit()


def get_person(person_number, is_get_childs):
    sorted_rows = common_df.loc[common_df['Номер личный'] == person_number].sort_values(by=['Год'],
                                                                                        ascending=False)
    person = sorted_rows.to_dict('records')[0]

    notnull_father_rows = sorted_rows[sorted_rows['Номер отца'].notnull()]
    if len(notnull_father_rows.index) != 0:
        person["Номер отца"] = notnull_father_rows['Номер отца'].iloc[0]

    notnull_age_rows = sorted_rows[sorted_rows['Возраст ныне'].notnull()]
    if len(notnull_age_rows) != 0 and len(notnull_father_rows) != 0:
        person["Год рождения"] = notnull_father_rows['Год'].iloc[0] - \
                                 notnull_father_rows['Возраст ныне'].iloc[0]

    if is_get_childs:
        childs = []
        for child in get_childs_list(person_number):
            childs.append(get_person(child, True))
        person["Дети"] = childs

    person.pop('Возраст ныне', None)
    person.pop('Год', None)
    person = {k: v for k, v in person.items() if v}
    return person


def get_ancestors_list(person_number):
    is_ancestor_exists = True
    ancestors = [person_number]
    while is_ancestor_exists:
        sorted_rows = common_df.loc[common_df['Номер личный'] == person_number].sort_values(by=['Год'],
                                                                                            ascending=False)

        notnull_father_rows = sorted_rows[sorted_rows['Номер отца'].notnull()]
        if len(notnull_father_rows.index) != 0:
            ancestors.append(notnull_father_rows['Номер отца'].iloc[0])
            person_number = notnull_father_rows['Номер отца'].iloc[0]
        else:
            is_ancestor_exists = False
    return ancestors


def get_childs_list(person_number):
    sorted_rows = common_df.loc[common_df['Номер отца'] == person_number]
    return set(sorted_rows['Номер личный'].tolist())


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
    return Response(response, mimetype=MIME_TYPE_JSON)


@app.route('/get_ancestors_tree', methods=['GET'])
def get_ancestors_tree():
    person_number = int(request.args.get('person_number'))

    tree = {}
    i = 0
    for ancestor in get_ancestors_list(person_number):
        person = get_person(ancestor, False)
        if i != 0:
            person["Дети"] = [tree]
        tree = person
        i = i + 1
    response = json.dumps(tree, ensure_ascii=False)
    return Response(response, mimetype=MIME_TYPE_JSON)


@app.route('/get_descendants_tree', methods=['GET'])
def get_descendants_tree():
    person_number = int(request.args.get('person_number'))

    tree = get_person(person_number, True)
    response = json.dumps(tree, ensure_ascii=False)
    return Response(response, mimetype=MIME_TYPE_JSON)


@app.route('/get_common_ancestors', methods=['GET'])
def get_common_ancestors():
    first_person_number = int(request.args.get('first_person_number'))
    second_person_number = int(request.args.get('second_person_number'))

    first_person_ancestors = get_ancestors_list(first_person_number)
    second_person_ancestors = get_ancestors_list(second_person_number)
    common_ancestors = list(set(first_person_ancestors) & set(second_person_ancestors))

    tree = {}
    i = 0
    for ancestor in common_ancestors:
        person = get_person(ancestor, False)
        if i != 0:
            person["Дети"] = [tree]
        tree = person
        i = i + 1
    response = json.dumps(tree, ensure_ascii=False)
    return Response(response, mimetype=MIME_TYPE_JSON)


def get_full_df(df):
    notna_df = df[df['Год рождения'].notna()]
    if len(np.unique(notna_df['Год рождения'])) != 1 and len(notna_df['Номер личный']) > 0:
        print("ВНИМАНИЕ: у человека #{0} различается год рождения между переписями - {1}"
              .format(notna_df['Номер личный'].iloc[0], list(notna_df['Год рождения'])))
    new_df = df[df['Год'] == df['Год'].max()]
    for index, row in df[df['Год рождения'].notna()].sort_values(by=['Год'], ascending=False).iterrows():
        if pd.isnull(new_df['Год рождения'].values[0]):
            new_df['Год рождения'] = row['Год рождения']
    return new_df


@app.route('/get_yearly_population', methods=['GET'])
def get_yearly_population():
    start_year = int(request.args.get('start_year'))
    end_year = int(request.args.get('end_year'))

    data = common_df
    data = data[~((data['Возраст ныне'].isnull()) & (data['Год выбытия'].isnull()))]
    data["Год рождения"] = data['Год'] - data['Возраст ныне']
    data = data.groupby('Номер личный').apply(get_full_df)

    year_to_population = pd.DataFrame(columns=['Год', 'Население'])
    for year in range(start_year, end_year + 1):
        year_data = data[data['Год рождения'] <= year]
        if year_data.size > 0:
            only_males_data = year_data[~year_data['Женщина']]
            only_males_data = only_males_data[(only_males_data['Год выбытия'] > year) | (only_males_data['Год выбытия'].isnull())]
            year_to_population = pd.concat(
                [year_to_population, pd.DataFrame.from_dict(dict(Год=[year], Население=[len(only_males_data)]))],
                ignore_index=True)
    return Response(year_to_population.to_csv(sep=',', index=False), mimetype=MIME_TYPE_CSV)


if __name__ == '__main__':
    prepare_data()

    serve(app, host="0.0.0.0", port=8080)
