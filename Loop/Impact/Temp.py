import MySQLdb
import collections
import bisect

if __name__=='__main__' and __package__ is None:
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from Outliers import Outliers
else:
    from ..Outliers import Outliers

mysql_cn = MySQLdb.connect(host='localhost', port=3306, user='root',
                           passwd='root',
                           db='digitalgreen',
                           charset='utf8',
                           use_unicode=True)

def onrun_query(query):
    cursor = mysql_cn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

get_percentile = Outliers.get_percentile

# To predict farmers' cost, we need correct data. This table provides correct data.
# {(aggregator_id, mandi_id, date): ()}
aggregator_wise_TCost_correct = Outliers.aggregator_wise_TCost_correct
# print len(aggregator_wise_TCost_correct)

# (Date, Aggregator ID, Market ID, Quantity, Vehicle Type, Transport Cost)
# Quantity is total quantity for date-aggregator-market combination even if multiple vehicles were taken.
aggregator_vehicle_market_data_query = 'SELECT ' \
'   dt.date, ' \
'   dt.user_created_id, ' \
'   dt.mandi_id, ' \
'   SUM(ct.quantity), ' \
'   tv.vehicle_id, ' \
'   dt.transportation_cost ' \
'FROM loop_daytransportation dt LEFT JOIN ' \
    'loop_combinedtransaction ct ON ct.date = dt.date ' \
    'AND ct.user_created_id = dt.user_created_id AND ct.mandi_id = dt.mandi_id ' \
    'LEFT JOIN loop_transportationvehicle tv ON dt.transportation_vehicle_id = tv.id ' \
'GROUP BY dt.id ORDER BY dt.date, dt.user_created_id, dt.mandi_id' \



aggregator_vehicle_market_data_query_result = onrun_query(aggregator_vehicle_market_data_query)
# print aggregator_vehicle_market_data_query_result


# The function can UPDATE values into a nested dictionary. To update, the entire path in nested dictionary format is needed.
# It can also create an ordered list if sort = 1
# Can't change a dictionary to list. So, the format of dictionary should NEVER change.
# TODO: Create recursive function to add elements to a list
def update_dictionary(dictionary_name, to_update, sort=1):
    for key, value in to_update.iteritems():
        if isinstance(value, collections.Mapping):
            temp = update_dictionary(dictionary_name.get(key, {}), value, sort)
            dictionary_name[key] = temp
        else:
            if sort == 0:
                #print '1'
                if key in dictionary_name.keys():
                    dictionary_name[key] = [dictionary_name[key]]
                    dictionary_name[key].append(to_update[key])
                else:
                    dictionary_name[key] = to_update[key]
            else:
                if key in dictionary_name.keys():
                    if isinstance(dictionary_name[key], (list, tuple)):
                        bisect.insort(dictionary_name[key], to_update[key])
                    else:
                        dictionary_name[key] = [dictionary_name[key]]
                        bisect.insort(dictionary_name[key], to_update[key])
                else:
                    dictionary_name[key] = to_update[key]
    return dictionary_name
# Updates min, max, average of aggregator-vehicle combination
# It is not used right now. Might be used later.
def update_min_max_average(quantity, aggregator_vehicle_min_max_average_quantity, aggregator_id, vehicle_type_id):
    if (aggregator_id, vehicle_type_id) in aggregator_vehicle_min_max_average_quantity:
        min_quantity = aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Min']
        max_quantity = aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Max']
        if quantity < min_quantity:
            aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Min'] = quantity
        elif quantity > max_quantity:
            aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Max'] = quantity
        aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Total Quantity'] += quantity
        aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Count'] += 1
        total_quantity = aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Total Quantity']
        total_count = aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Count']
        aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)][
            'Avg'] = total_quantity / total_count
    else:
        aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)] = {'Min': quantity,
                                                                                         'Max': quantity,
                                                                                         'Avg': quantity,
                                                                                         'Total Quantity': quantity,
                                                                                         'Count': 1}

aggregator_market_date_single_vehicle_data = {}
aggregator_market_date_multiple_vehicle_data = {}
aggregator_vehicle_market_limit = {}
aggregator_vehicle_market_data = {}
aggregator_vehicle_min_max_average_quantity = {}
aggregator_vehicle_data = {}

# To store values of previous row
class PreviousVariables:
    previous_quantity = 0
    previous_aggregator_id = 0
    previous_vehicle_type_id = 0
    previous_is_single = 1
    previous_key = ()
    previous_value = []
    previous_avm_update_value = {}
    previous_av_update_value = {}

previous = PreviousVariables()


def update_previous_variables(name, q, a_id, v_id, key, value, avm_update_value, av_update_value):
    name.previous_quantity = q
    name.previous_aggregator_id = a_id
    name.previous_vehicle_type_id = v_id
    name.previous_key = key
    name.previous_value = value
    name.previous_avm_update_value = avm_update_value
    name.previous_av_update_value = av_update_value

# Finds min-max-average for aggregator-vehicle combination
# Create two separate dictionary for single vehicle and multiple vehicles per visit
# Updates aggregator_vehicle_data which would be used later for predicting farmers' quantity
# Updates aggregator_vehicle_market_data which would be later used for predicting farmers' cost
#
# print 'Total'
# print len(aggregator_vehicle_market_data_query_result)

count = 0
for row in aggregator_vehicle_market_data_query_result:
    date = row[0]
    aggregator_id = row[1]
    market_id = row[2]
    quantity = row[3]
    vehicle_type_id = row[4]
    transport_cost = row[5]
    key = (aggregator_id, market_id, date)
    value = [vehicle_type_id, quantity, transport_cost]
    avm_update_value = {aggregator_id: {vehicle_type_id: {market_id: transport_cost}}}
    av_update_value = {aggregator_id: {vehicle_type_id: quantity}}
    # print key

    if key in aggregator_wise_TCost_correct: # To exclude outliers from computation
        # When this visit has >1 vehicles
        if previous.previous_key == key and previous.previous_is_single == 1:
            aggregator_market_date_multiple_vehicle_data[previous.previous_key] = [previous.previous_value]
            update_previous_variables(previous, quantity, aggregator_id, vehicle_type_id, key, value, avm_update_value, av_update_value)
            previous.previous_is_single = 0
        # When this visit has >2 vehicles
        elif previous.previous_key == key and previous.previous_is_single == 0:
            aggregator_market_date_multiple_vehicle_data[previous.previous_key].append(previous.previous_value)
            update_previous_variables(previous, quantity, aggregator_id, vehicle_type_id, key, value, avm_update_value, av_update_value)
            previous.previous_is_single = 0
        # When last visit has multiple vehicles
        elif previous.previous_key and previous.previous_is_single == 0:
            aggregator_market_date_multiple_vehicle_data[previous.previous_key].append(previous.previous_value)
            update_previous_variables(previous, quantity, aggregator_id, vehicle_type_id, key, value, avm_update_value, av_update_value)
            previous.previous_is_single = 1
        # When last visit has single vehicle
        elif previous.previous_key and previous.previous_is_single == 1:
            aggregator_market_date_single_vehicle_data[previous.previous_key] = previous.previous_value
            update_min_max_average(previous.previous_quantity, aggregator_vehicle_min_max_average_quantity, previous.previous_aggregator_id, previous.previous_vehicle_type_id)
            update_dictionary(aggregator_vehicle_market_data, previous.previous_avm_update_value)
            update_dictionary(aggregator_vehicle_data, previous.previous_av_update_value)
            update_previous_variables(previous, quantity, aggregator_id, vehicle_type_id, key, value, avm_update_value, av_update_value)
            previous.previous_is_single = 1
        # When previous_key is empty i.e. only at the first row
        else:
            update_previous_variables(previous, quantity, aggregator_id, vehicle_type_id, key, value, avm_update_value, av_update_value)

# print 'Single'
# print len(aggregator_market_date_single_vehicle_data)
# print 'Multiple'
# print len(aggregator_market_date_multiple_vehicle_data)

# Predicts quantity taken in a vehicle on days when multiple vehicles were taken. Inserts it into avm-data and av-data
for keys in aggregator_market_date_multiple_vehicle_data.keys():
    aggregator_id = keys[0]
    market_id = keys[1]
    date = keys[2]
    vehicle_type_col = 0
    quantity_col = 1
    transport_cost_col = 2
    sum_average = 0

    for row in aggregator_market_date_multiple_vehicle_data[keys]:
        vehicle_type_id = row[vehicle_type_col]
        if (aggregator_id, vehicle_type_id) in aggregator_vehicle_min_max_average_quantity.keys():
            sum_average += aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Avg']
        else:
            sum_average = 0
            break
    for row in aggregator_market_date_multiple_vehicle_data[keys]:
        if sum_average == 0:
            break
        vehicle_type_id = row[vehicle_type_col]
        total_quantity = row[quantity_col]
        transport_cost = row[transport_cost_col]

        vehicle_average_quantity = aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Avg']
        predicted_quantity = (vehicle_average_quantity * total_quantity) / sum_average
        update_min_max_average(predicted_quantity,aggregator_vehicle_min_max_average_quantity,aggregator_id,vehicle_type_id)

        avm_update_value = {aggregator_id: {vehicle_type_id: {market_id: transport_cost}}}
        av_update_value = {aggregator_id: {vehicle_type_id: predicted_quantity}}
        update_dictionary(aggregator_vehicle_market_data, avm_update_value)
        update_dictionary(aggregator_vehicle_data, av_update_value)

# Finds Quantity threshold for each a-v combination and Cost for each a-v-m combination. Inserts it for a-v-m.
# TODO: Create better algorithms for identification of thresholds

for aggregator_id in aggregator_vehicle_data.keys():
    print aggregator_id
    vehicle_market_limit_ordered = collections.OrderedDict()
    vehicle_market_limit = {}
    qlimit_vehicle_tuple_data = []
    for vehicle_type_id in aggregator_vehicle_data[aggregator_id].keys():
        # TODO: Remove jugaad
        a_v_dataset = aggregator_vehicle_data[aggregator_id][vehicle_type_id]
        if not isinstance(a_v_dataset, (list, tuple)):
            aggregator_vehicle_data[aggregator_id][vehicle_type_id] = [a_v_dataset]
            a_v_dataset = aggregator_vehicle_data[aggregator_id][vehicle_type_id]
        length = len(a_v_dataset)
        q_limit = get_percentile(a_v_dataset, -1, 0, length, 0.75)
        aggregator_vehicle_min_max_average_quantity[(aggregator_id, vehicle_type_id)]['Q Limit'] = q_limit
        bisect.insort(qlimit_vehicle_tuple_data, (q_limit, vehicle_type_id))
        for market_id in aggregator_vehicle_market_data[aggregator_id][vehicle_type_id].keys():
            # TODO: Remove jugaad
            a_v_m_dataset = aggregator_vehicle_market_data[aggregator_id][vehicle_type_id][market_id]
            if not isinstance(a_v_m_dataset, (list, tuple)):
                aggregator_vehicle_market_data[aggregator_id][vehicle_type_id][market_id] = [a_v_m_dataset]
                a_v_m_dataset = aggregator_vehicle_market_data[aggregator_id][vehicle_type_id][market_id]
            length = len(a_v_m_dataset)
            predicted_cost = get_percentile(a_v_m_dataset, -1, 0, length, 0.5)
            vehicle_market_limit_update = {(q_limit, vehicle_type_id): {market_id: {'Cost': predicted_cost, 'Visits': length}}}
            update_dictionary(vehicle_market_limit, vehicle_market_limit_update, 0)
    for elements in qlimit_vehicle_tuple_data:
        update_dictionary(vehicle_market_limit_ordered, {elements: vehicle_market_limit[elements]}, 0)
    aggregator_vehicle_market_limit[aggregator_id] = vehicle_market_limit_ordered
    # print aggregator_vehicle_market_limit[aggregator_id]
    #print vehicle_market_limit_ordered
            # print aggregator_id, vehicle_type_id, market_id, length, predicted_cost, a_v_m_limit_update


# count = 0
# for keys in aggregator_vehicle_market_data.keys():
#     for keys_1 in aggregator_vehicle_market_data[keys].keys():
#         for keys_2 in aggregator_vehicle_market_data[keys][keys_1].keys():
#             if count < 10:
#                 print aggregator_vehicle_market_data[keys][keys_1][keys_2]
#                 count +=1
#
# print count
#
# for line in aggregator_vehicle_market_data_query_result:
#     if count < 20:
#         print line
#         count += 1


# count = 0
# for keys in aggregator_vehicle_min_max_average_quantity:
#     count += aggregator_vehicle_min_max_average_quantity[keys]['Count']
# print count
# count = 0
#
# for keys in aggregator_vehicle_data.keys():
#     print 'A', keys
#     for keys_1 in aggregator_vehicle_data[keys].keys():
#         print 'V', keys_1
#         # for keys_2 in aggregator_vehicle_market_data[keys][keys_1].keys():
#         #     print 'M', keys_2
#         lines = aggregator_vehicle_data[keys][keys_1]
#         print lines
#         count += len(aggregator_vehicle_market_data[keys][keys_1])
# print count
# print aggregator_vehicle_market_data
# print aggregator_market_date_multiple_vehicle_data
# print aggregator_market_date_single_vehicle_data

# date_col = 0
# user_id_col = 1
# mandi_id_col = 2
#
# vehicle_count_col = 7
#
#
#
# aggregator_vehicle_market_data_final = {}
#
# average_count = 0
# min = 0
# max = 0
#
#
#
#
# for keys in a_m_single_vehicle_days.keys():
#     aggregator_id = keys[0]
#     market_id = keys[1]
#     vehicle_type_col = 4
#     quantity_col = 3
#     transport_cost_col = 5
#     for date in a_m_single_vehicle_days[keys]:
#         vehicle_type_id = aggregator_vehicle_market_data[(aggregator_id, market_id,date)][0][vehicle_type_col]
#         quantity = aggregator_vehicle_market_data[(aggregator_id, market_id,date)][0][quantity_col]
#         transport_cost = aggregator_vehicle_market_data[(aggregator_id, market_id, date)][0][transport_cost_col]
#         update_min_max_average(quantity, aggregator_vehicle_min_max_average_quantity, aggregator_id, vehicle_type_id)
#         if aggregator_id in aggregator_vehicle_market_data_final.keys():
#             if vehicle_type_id in aggregator_vehicle_market_data_final[aggregator_id].keys():
#                 if market_id in aggregator_vehicle_market_data_final[aggregator_id][vehicle_type_id].keys():
#                     aggregator_vehicle_market_data_final[aggregator_id][vehicle_type_id][market_id].append((quantity, transport_cost))
#                 else:
#                     aggregator_vehicle_market_data_final[aggregator_id][vehicle_type_id][market_id] = [(quantity, transport_cost)]
#             else:
#                 aggregator_vehicle_market_data_final[aggregator_id][vehicle_type_id] = {market_id: [(quantity, transport_cost)]}
#         else:
#             aggregator_vehicle_market_data_final[aggregator_id] = {vehicle_type_id: {market_id: [(quantity, transport_cost)]}}
#
# print aggregator_vehicle_min_max_average_quantity
#
#
# print aggregator_vehicle_market_data_final
# print aggregator_vehicle_min_max_average_quantity


# for aggregator_id in aggregator_vehicle_market_data_final:
#     for vehicle_type_id in aggregator_vehicle_market_data_final[aggregator_id]:
#         for market_id in aggregator_vehicle_market_data_final[aggregator_id][vehicle_type_id]:

#





    # import xlsxwriter
#
# format = {'date_format': {'num_format': 'd mmm yyyy'}}
#
#
# # Creates a workbook
# def create_workbook(workbook_name):
#     workbook = xlsxwriter.Workbook(workbook_name)
#     return workbook
#
#
# # Creates all formats in the workbook
# def create_format(list_of_format, workbook):
#     formats_created = {}
#     for formats in list_of_format:
#         formats_created[formats] = workbook.add_format(format[formats])
#     return formats_created
#
#
# # Purpose: Takes a xlsx file as parameter and enters data in it
# # Parameters: workbook - name of xlsx file e.g. 'xyz.xlsx'
# #             sheets_data - dictionary with key = sheet name and value = list of list of sheets data e.g. {'Dev': [[1,2],[3,4]]}
# #             table_properties - dictionary with properties of xlsx writer. No default properties right now. Keep Data field - None.
# #             table_position - contains row, col of first cell of table e.g. {'row': 0, 'col': 0}
# #             column_width_and_format - Dictionary of all column widths. {'A:A': 9.5}
# #             sheet name should be string
#
# default_table_position = {'row': 0, 'col': 0}
#
#
# def create_xlsx(workbook, sheets_data, table_properties, table_position=default_table_position,
#                 column_width_and_format=None):
#     sheet_name = {}
#     for keys in sheets_data.keys():
#         rows_count = len(sheets_data[keys])
#         columns_count = len(sheets_data[keys][0]) - 1
#         sheet_name[keys] = workbook.add_worksheet(str(keys))
#         row_position = table_position['row']
#         col_position = table_position['col']
#         table_properties['data'] = sheets_data[keys]
#         sheet_name[keys].add_table(row_position, col_position, rows_count, columns_count, table_properties)
#         for keys1 in column_width_and_format.keys():
#             sheet_name[keys].set_column(keys1, column_width_and_format[keys1])
#
#     workbook.close()
#
# workbook = create_workbook('Transport Cost and Quantity.xlsx')
# all_format = ['date_format']
# all_format_created = create_format(all_format, workbook)
# column_properties = [{'header': 'Date', 'format': all_format_created['date_format']}, {'header': 'Aggregator'},
#                      {'header': 'Market'}, {'header': 'Quantity'}, {'header': 'TCost'}, {'header': 'TCPK'},
#                      {'header': 'FShare'}, {'header': 'FCount'}]
# table_properties = {'data': None, 'autofilter': False, 'banded_rows': False, 'style': 'Table Style Light 15',
#                     'columns': column_properties}
# column_width = {'A:A': 10.55, 'B:B': 9.36}
# create_xlsx(workbook, a_m_single_vehicle_days, table_properties, column_width_and_format=column_width)

# The function can DELETE elements from a nested dictionary. To delete, the entire path in nested dictionary format is needed.
# The element must exist in the dictionary. No error handling done. Not used right now.
# def delete_from_dictionary(dictionary_name, to_delete):
#     for key, value in to_delete.iteritems():
#         if isinstance(value, collections.Mapping):
#             temp = delete_from_dictionary(dictionary_name.get(key, {}), value)
#             print temp
#             if len(temp) == 0:
#                 del dictionary_name[key]
#         else:
#             if key in dictionary_name.keys():
#                 if len(dictionary_name[key]) > len(to_delete[key]):
#                     for elements in to_delete[key]:
#                         dictionary_name[key].remove(elements)
#                 elif dictionary_name[key] == to_delete[key]:
#                     del dictionary_name[key]
#     return dictionary_name


