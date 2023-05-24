from src.operators.xlsx_report_plugin import ExcelReportPlugin
import os


base_path = os.sep.join(os.getcwd().split(os.sep)[:-2])

print(base_path)

input_file = base_path + "/env/input_data/supermarket_sales.xlsx"
output_file = base_path + "/env/output_data/report_penjualan_2019.xlsx"

automate = ExcelReportPlugin(input_file, output_file)

automate.run()