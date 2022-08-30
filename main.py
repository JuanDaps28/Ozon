import os
import json
import uuid
import numpy as np
import pandas as pd
from datetime import datetime as dt


def read_brands_data() -> pd.DataFrame:
    brands_data_path = os.path.join(os.getcwd(), 'rawData', 'brands.csv')
    brands_df = pd.read_csv(brands_data_path)[['_id', 'name']]
    return brands_df


def read_motos_data(brands_df: pd.DataFrame) -> pd.DataFrame:
    motos_data_path = os.path.join(os.getcwd(), 'rawData', 'BD Motos.xlsx')
    motos_df = pd.read_excel(motos_data_path, header=1)[['cuota', 'Precio venta descuento', 'cuota semanal descuento', 'año ', 'kilometraje_aprox', 'id_ozon', 'serie_vehicular_o_num_chasis', 'num_motor', 'gasto_compra', 'Color', 'cilindraje', 'pais', 'placa', 'num_tarjeta_circ', 'marca']]
    motos_df = motos_df[(motos_df['pais'] == 'mexico') & (motos_df['id_ozon'])]
    motos_df = motos_df[motos_df['id_ozon'].apply(lambda x: True if int(x[3:]) < 1000 else False)]
    motos_df['marca'] = motos_df['marca'].apply(lambda x: x if pd.isna(x) else str(x).upper())
    motos_df = pd.merge(motos_df, brands_df, how='left', left_on=['marca'], right_on=['name']).drop(columns=['name'])
    motos_df = motos_df.replace({np.nan: None})
    return motos_df


def get_uuid() -> str:
    return str(uuid.uuid4())


def get_details(year: float, milage: float) -> dict:
    return {'year': year, 'milage': milage}


def get_mongo_data_df(motos_df: pd.DataFrame) -> pd.DataFrame:
    mongo_data_df = pd.DataFrame()
    mongo_data_df['salePrice'] = motos_df.apply(lambda x: x['cuota'] if pd.isna(x['Precio venta descuento']) else x['cuota semanal descuento'], axis=1)
    mongo_data_df['oldPrice'] = motos_df.apply(lambda x: x['cuota'] if pd.isna(x['Precio venta descuento']) else 0, axis=1)
    mongo_data_df['internalId'] = motos_df['id_ozon']
    mongo_data_df['vehicleSN'] = motos_df['serie_vehicular_o_num_chasis']
    mongo_data_df['engineSN'] = motos_df['num_motor']
    mongo_data_df['purchaseCost'] = motos_df['gasto_compra']
    mongo_data_df['color'] = motos_df['Color']
    mongo_data_df['cylindersCapacity'] = motos_df['cilindraje']
    mongo_data_df['brand'] = motos_df['_id']
    mongo_data_df['createdAt'] = mongo_data_df.apply(lambda x: dt.now().isoformat(), axis=1)
    mongo_data_df['updatedAt'] = mongo_data_df.apply(lambda x: dt.now().isoformat(), axis=1)
    mongo_data_df['country'] = motos_df['pais']
    mongo_data_df['plate'] = motos_df['placa']
    mongo_data_df['registrationCard'] = motos_df['num_tarjeta_circ']
    mongo_data_df['details'] = motos_df.apply(lambda x: get_details(x['año '], x['kilometraje_aprox']), axis=1)
    mongo_data_df['_id'] = mongo_data_df.apply(lambda x: get_uuid(), axis=1)
    mongo_data_df = mongo_data_df.replace({np.nan: None})
    return mongo_data_df


def generate_mongo_data_json(mongo_data_df: pd.DataFrame, file_name: str) -> None:
    mongo_data = mongo_data_df.to_dict('records')
    with open(os.path.join(os.getcwd(), 'mongoData', file_name+'.json'), "w") as outfile:
        outfile.write(json.dumps(mongo_data))


def main():
    brands_df = read_brands_data()
    motos_df = read_motos_data(brands_df)
    mongo_data_df = get_mongo_data_df(motos_df)
    generate_mongo_data_json(mongo_data_df, 'mongoData')


main()