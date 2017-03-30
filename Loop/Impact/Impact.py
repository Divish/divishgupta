# Contains function definitions, tables from database
import Functions, Tables, farmer_predict_cost
import csv

if (__name__== '__main__' or __name__=='Impact') and __package__ is None:
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from Outliers import write_xlsx
else:
    from ..Outliers import write_xlsx

create_format = write_xlsx.create_format
create_workbook = write_xlsx.create_workbook
create_xlsx = write_xlsx.create_xlsx

aggregator_local_market_data = Tables.aggregator_local_market_data
farmer_time_cost_prediction_model = farmer_predict_cost.aggregator_vehicle_market_limit
daily_farmer_predicted_cost_time_data = Tables.daily_farmer_predicted_cost_time_data
daily_aggregator_market_data = Tables.daily_aggregator_market_data
daily_aggregator_market_ca_data = Tables.daily_aggregator_market_ca_data
daily_crop_market_data = Tables.daily_crop_market_data
transaction_level_data = Tables.transaction_level_data

# Step 1: Append Predicted Cost for each Date Farmer combination in the table on the basis of quantity.
# TODO: Also include predicted time in daily_farmer_.... data
# TODO: aggregator_local_market_data should also be stored in database

# Local markets are defined aggregator-wise which might not work in future when an aggregator is covering a larger area

append_predicted_cost_in_daily_farmer_data = Functions.append_predicted_cost_in_daily_farmer_data
aggregator_id_col = 1
farmer_id_col = 2
for key in daily_farmer_predicted_cost_time_data.keys():
    farmer_id = key[farmer_id_col]
    aggregator_id = key[aggregator_id_col]
    quantity = daily_farmer_predicted_cost_time_data[key]['Quantity']
    local_market_list = aggregator_local_market_data.get(aggregator_id, {})
    farmer_time_cost_prediction_model_list = farmer_time_cost_prediction_model.get(aggregator_id, {})
    daily_farmer_predicted_cost_time_data[key] = append_predicted_cost_in_daily_farmer_data(
    daily_farmer_predicted_cost_time_data[key], farmer_time_cost_prediction_model_list, local_market_list)
    # print daily_farmer_predicted_cost_time_data[key]

# Step 2: Append aggregator time for each D-A-M combination in the table on the basis of A-M
# Step 3: Append predicted time for each Date Farmer combination in the table on the basis of quantity.
#
# Step 4: Append impact in each transaction by picking values from different tables
append_impact_in_transaction_level_data = Functions.append_impact_in_transaction
for lines in transaction_level_data:
    date = lines['D']
    farmer_id = lines['F']
    aggregator_id = lines['A']
    market_id = lines['M']
    crop_id = lines['C']
    ca_id = lines['CA']
    line_daily_farmer_predicted_cost_time = daily_farmer_predicted_cost_time_data[(date, aggregator_id, farmer_id)]
    line_daily_aggregator_market = daily_aggregator_market_data[(date, aggregator_id, market_id)]
    line_daily_aggregator_market_ca = daily_aggregator_market_ca_data[(date, aggregator_id, market_id, ca_id)]

    # TODO: Variable names could be something better
    farmer_time_cost_prediction_model_list = farmer_time_cost_prediction_model.get(aggregator_id, {})
    local_market_list = aggregator_local_market_data.get(aggregator_id, {})
    daily_crop_market_list = daily_crop_market_data[(date, crop_id)]
    lines = append_impact_in_transaction_level_data(lines, line_daily_farmer_predicted_cost_time,
                                                    line_daily_aggregator_market, line_daily_aggregator_market_ca,
                                                    farmer_time_cost_prediction_model_list, local_market_list, daily_crop_market_list)
    #print lines
# for keys in farmer_time_cost_prediction_model:
#     print keys, farmer_time_cost_prediction_model[keys]
# convert_in_excel = {'Impact': transaction_level_data}
# # TODO: Use xlsx writer function
with open('loop impact.csv', 'w') as csvfile:
    fieldnames = ['D', 'A', 'M', 'CA', 'F', 'C', 'Q', 'R', 'Average R', 'TCPK', 'ACPK', 'FSPK', 'CASPK',
                  'Predicted CPK', 'DG Cost', 'Profit - Transport', 'Profit - Rate']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for lines in transaction_level_data:
        writer.writerow(lines)

# workbook = create_workbook('Impact.xlsx')
# all_format = ['date_format']
# all_format_created = create_format(all_format, workbook)
# column_properties = [{'header': 'Date', 'format': all_format_created['date_format']}, {'header': 'Aggregator'},
#                      {'header': 'Market'}, {'header': 'Gaddidar'}, {'header': 'Farmer'}, {'header': 'Crop'},
#                      {'header': 'Quantity'}, {'header': 'Rate'}, {'header': 'Average Rate'}, {'header': 'TCPK'},
#                      {'header': 'Aggregator Incentive'}, {'header': 'Farmer Share PK'}, {'header': 'Gaddidar Share PK'},
#                      {'header': 'Predicted CPK'}]
# table_properties = {'data': None, 'autofilter': False, 'banded_rows': False, 'style': 'Table Style Light 15',
#                     'columns': column_properties}
# column_width = {'A:A': 10.55, 'B:B': 9.36}
# create_xlsx(workbook, convert_in_excel, table_properties, column_width_and_format=column_width)

# ____________________________________________________________________________________________________________________
# aggregator_market_time = Tables.aggregator_market_time
# aggregator_market_data = Tables.aggregator_market_data
# aggregator_ca_data = Tables.aggregator_ca_data



# # It should come directly from aggregator_market_time because aggregator_market_data will always store average time
# append_time_in_real_dam = Functions.append_time_in_real_dam
# for key in daily_aggregator_market_data.keys():
#     aggregator_id = key[1]
#     market_id = key[2]
#     daily_aggregator_market_data[key] = append_time_in_real_dam(daily_aggregator_market_data[key], aggregator_market_time.get((aggregator_id, market_id), []))
