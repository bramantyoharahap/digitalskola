import logging
import pandas as pd
import numpy as np
from sqlalchemy.exc import SQLAlchemyError
# from pyspark.sql import SparkSession

class Transformer():
    tbl_covid_jabar = 'covid_jabar'
    tbl_dim_province = 'dim_province'
    tbl_dim_district = 'dim_district'
    tbl_dim_case = 'dim_case'
    tbl_province_daily = 'province_daily'
    tbl_district_daily = 'district_daily'

    def __init__(self, mysql_engine, pg_engine):
        self.mysql_engine = mysql_engine
        self.pg_engine = pg_engine

    def get_covidjabar_data(self):
        sql = f'SELECT * FROM {self.tbl_covid_jabar}'
        df = pd.read_sql(sql=sql, con=self.mysql_engine)
        logging.info(f'GET DATA FROM TABLE {self.tbl_covid_jabar} SUCCESS')
        return df
    
    def get_dim_case_data(self):
        sql = f'SELECT * FROM {self.tbl_dim_case}'
        df = pd.read_sql(sql=sql, con=self.pg_engine)
        logging.info(f'GET DATA FROM TABLE {self.tbl_dim_case} SUCCESS')
        return df
    
    def insert_data_to_dim_province(self):
        df_covidjabar = self.get_covidjabar_data()
        df_province = df_covidjabar[['kode_prov','nama_prov']]
        print(df_province)
        df_province = df_province.rename(columns={'kode_prov':'province_id', 'nama_prov':'province_name'})
        df_province = df_province.drop_duplicates()
        print(df_province)
        try:
            drop_table_dim_province = f'DROP TABLE IF EXISTS {self.tbl_dim_province}'
            self.pg_engine.execute(drop_table_dim_province)
        except SQLAlchemyError as ex:
            logging.error(ex)

        df_province.to_sql(con=self.pg_engine, name=f'{self.tbl_dim_province}', index=False)
        logging.info(f'DATA INSERTED SUCCESS TO TABLE {self.tbl_dim_province}')

    def insert_data_to_dim_district(self):
        df_covidjabar = self.get_covidjabar_data()
        df_district = df_covidjabar[['kode_kab', 'kode_prov', 'nama_kab']]
        print(df_district)
        df_district = df_district.rename(
            columns={
                'kode_kab':'district_id', 
                'kode_prov':'province_id',
                'nama_kab':'district_name'
            })
        df_district = df_district.drop_duplicates()
        print(df_district)
        try:
            drop_table_dim_district = f'DROP TABLE IF EXISTS {self.tbl_dim_district}'
            self.pg_engine.execute(drop_table_dim_district)
        except SQLAlchemyError as ex:
            logging.error(ex)

        df_district.to_sql(con=self.pg_engine, name=f'{self.tbl_dim_district}', index=False)
        logging.info(f'DATA INSERTED SUCCESS TO TABLE {self.tbl_dim_district}')

    def insert_data_to_dim_case(self):
        
        temp = [
            'closecontact_dikarantina',
            'closecontact_discarded',
            'closecontact_meninggal',
            'confirmation_meninggal',
            'confirmation_sembuh',
            'probable_diisolasi',
            'probable_discarded',
            'probable_meninggal',
            'suspect_diisolasi',
            'suspect_discarded',
            'suspect_meninggal'
        ]

        data = {
            'id': [i for i in range(1,len(temp)+1)], 
            'status_name': [i.split('_')[0] for i in temp], 
            'status_detail': [i.split('_')[1] for i in temp], 
            'status': temp
        }

        df_case = pd.DataFrame(data)

        try:
            drop_table_tbl_dim_case = f'DROP TABLE IF EXISTS {self.tbl_dim_case}'
            self.pg_engine.execute(drop_table_tbl_dim_case)
        except SQLAlchemyError as ex:
            logging.error(ex)

        df_case.to_sql(con=self.pg_engine, name=f'{self.tbl_dim_case}', index=False)
        logging.info(f'DATA INSERTED SUCCESS TO TABLE {self.tbl_dim_case}')

    def insert_data_to_province_daily(self):
        df_covidjabar = self.get_covidjabar_data()
        
        df_dim_case = self.get_dim_case_data()

        column_start = [
            'tanggal', 'kode_prov',
            'closecontact_dikarantina',
            'closecontact_discarded',
            'closecontact_meninggal',
            'confirmation_meninggal',
            'confirmation_sembuh',
            'probable_diisolasi',
            'probable_discarded',
            'probable_meninggal',
            'suspect_diisolasi',
            'suspect_discarded',
            'suspect_meninggal'
        ]

        column_end = ['date', 'province_id', 'status', 'total']

        data = df_covidjabar[column_start]
        data = data.melt(id_vars=['tanggal', 'kode_prov'], var_name='status', value_name='total').sort_values(['tanggal','kode_prov'])
        data = data.groupby(['tanggal','kode_prov', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_dim_case = df_dim_case.rename({'id':'case_id'}, axis=1)

        data = pd.merge(data, df_dim_case, how='inner', on='status')
        data = data[['id', 'province_id', 'case_id', 'date', 'total']]

        try:
            drop_table = f'DROP TABLE IF EXISTS {self.tbl_province_daily}'
            self.pg_engine.execute(drop_table)
        except SQLAlchemyError as ex:
            logging.error(ex)

        data.to_sql(con=self.pg_engine, name=f'{self.tbl_province_daily}', index=False)
        logging.info(f'DATA INSERTED SUCCESS TO TABLE {self.tbl_province_daily}')

    def insert_data_to_district_daily(self):
        df_covidjabar = self.get_covidjabar_data()
        
        df_dim_case = self.get_dim_case_data()

        column_start = [
            'tanggal', 'kode_kab',
            'closecontact_dikarantina',
            'closecontact_discarded',
            'closecontact_meninggal',
            'confirmation_meninggal',
            'confirmation_sembuh',
            'probable_diisolasi',
            'probable_discarded',
            'probable_meninggal',
            'suspect_diisolasi',
            'suspect_discarded',
            'suspect_meninggal'
        ]

        column_end = ['date', 'district_id', 'status', 'total']

        data = df_covidjabar[column_start]
        data = data.melt(id_vars=['tanggal', 'kode_kab'], var_name='status', value_name='total').sort_values(['tanggal','kode_kab'])
        data = data.groupby(['tanggal','kode_kab', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_dim_case = df_dim_case.rename({'id':'case_id'}, axis=1)

        data = pd.merge(data, df_dim_case, how='inner', on='status')
        data = data[['id', 'district_id', 'case_id', 'date', 'total']]

        try:
            drop_table = f'DROP TABLE IF EXISTS {self.tbl_district_daily}'
            self.pg_engine.execute(drop_table)
        except SQLAlchemyError as ex:
            logging.error(ex)

        data.to_sql(con=self.pg_engine, name=f'{self.tbl_district_daily}', index=False)
        logging.info(f'DATA INSERTED SUCCESS TO TABLE {self.tbl_district_daily}')