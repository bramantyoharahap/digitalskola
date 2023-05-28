
import pandas as pd
import json
import string
import discord

from openpyxl import load_workbook
from openpyxl.styles import *
from openpyxl.chart import *
from openpyxl.chart.shapes import GraphicalProperties
from openpyxl.chart.label import DataLabelList
from openpyxl.worksheet.dimensions import ColumnDimension, DimensionHolder
from openpyxl.utils import get_column_letter
from discord import SyncWebhook

settings = json.load(open('../settings.json'))
webhook_url = settings['webhook_url']

class ExcelReportPlugin():

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def run(self):
        df = self.read_input_file()
        df_transform = self.transform2(df)
        self.create_output_file(df_transform)

        wb = load_workbook(self.output_file)
        wb.active = wb['Report']

        min_column = wb.active.min_column
        max_column = wb.active.max_column
        min_row = wb.active.min_row
        max_row = wb.active.max_row

        self.barchart(wb.active, min_column, max_column, min_row, max_row)
        self.add_total(wb.active, max_column, max_row, min_row)
        self.resolve_column_width(wb.active)
        self.save_file(wb)
        self.send_to_discord(webhook_url)

    def read_input_file(self):
        df = pd.read_excel(self.input_file)
        return df
    
    def transform1(self, df_transform):
        df_transform = df_transform.pivot_table(index='Gender', 
                    columns='Product line', 
                    values='Total', 
                    aggfunc='sum').round()
        
        return df_transform
    
    def transform2(self, df_transform):
        df_transform['Date'] = pd.to_datetime(df_transform["Date"]).dt.strftime('%Y-%m-%d')
        df_transform = df_transform.pivot_table(index=['Gender','Date'], 
                    columns='Product line', 
                    values='Total', 
                    aggfunc='sum').round()
        
        return df_transform
        
    def create_output_file(self, df):
        df.to_excel(self.output_file, 
                sheet_name='Report', 
                startrow=4)

    def barchart(self, workbook, min_column, max_column, min_row, max_row):
        barchart = BarChart()

        data = Reference(workbook, 
                         min_col=min_column+2,
                         max_col=max_column,
                         min_row=min_row,
                         max_row=max_row
                        )

        categories = Reference(workbook,
                                min_col=min_column+1,
                                max_col=min_column,
                                min_row=min_row+1,
                                max_row=max_row
                                )

        barchart.add_data(data, titles_from_data=True)
        barchart.set_categories(categories)
        barchart.gapWidth = 500


        workbook.add_chart(barchart, 'J5')
        barchart.title = 'Sales berdasarkan Produk'
        barchart.style = 2
        barchart.height = 20
        barchart.width = 150
        
    def add_total(self, workbook, max_column, max_row, min_row):
        alphabet = list(string.ascii_uppercase)
        alphabet_excel = alphabet[:max_column]
        #[A,B,C,D,E,F,G]
        for i in alphabet_excel:
            if i != 'A' and i != 'B':
                workbook[f'{i}{max_row+1}'] = f'=SUM({i}{min_row+1}:{i}{max_row})'
                workbook[f'{i}{max_row+1}'].style = 'Currency'

        workbook[f'{alphabet_excel[0]}{max_row+1}'] = 'Total'

        workbook[f'{alphabet_excel[1]}{max_row+1}'] = f'=SUM({alphabet_excel[2]}{max_row+1}:{alphabet_excel[7]}{max_row+1})'
        workbook[f'{alphabet_excel[1]}{max_row+1}'].style = 'Currency'

        workbook['A1'] = 'Sales Report'
        workbook['A2'] = '2019'
        workbook['A1'].font = Font('Arial', bold=True, size=20)
        workbook['A2'].font = Font('Arial', bold=True, size=10)

    def resolve_column_width(self, workbook):
        for col in workbook.columns:
            max_length = 0
            column = col[0].column_letter # Get the column name
            for cell in col:
                try: # Necessary to avoid error on empty cells
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2) * 0.9
            workbook.column_dimensions[column].width = adjusted_width

        
    def save_file(self, wb):
        wb.save(self.output_file)

    def send_to_discord(self, webhook_url):
        webhook = SyncWebhook.from_url(webhook_url)

        with open(file=self.output_file, mode='rb') as file:
            excel_file = discord.File(file)

        webhook.send('This is an automated report', 
                    username='Report Hook', 
                file=excel_file)

