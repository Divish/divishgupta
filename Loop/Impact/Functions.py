import collections
import bisect


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

# Purpose: To read quantity from daily_farmer_predicted_cost_time_data table and appends predicted cost and predicted time in it.
# TODO: Include time in it.
# TODO: Handle case when quantity is greater than quantity of all vehicles. Not sure if it will ever happen.
def append_predicted_cost_in_daily_farmer_data(line_dict_farmer_date, farmer_time_cost_model_list, local_markets_data):
    f_cost = 0
    total_count = 0.0
    total_quantity = line_dict_farmer_date['Quantity']
    max_quantity_col = 0
    # Checks if this aggregator exists in models and if local markets are known. If not, then else.
    # print local_markets_data, farmer_time_cost_model_list
    if total_quantity < 60:
        f_cost = 60.0
    elif local_markets_data and farmer_time_cost_model_list:
        for keys in farmer_time_cost_model_list.keys():
            max_quantity = keys[max_quantity_col]
            if total_quantity < max_quantity:
                market_list = farmer_time_cost_model_list[keys]
                # Total count gets total number of visits made in all local markets
                for market_id in local_markets_data:
                    if market_id in market_list:
                        total_count += market_list[market_id]['Visits']
                for market_id in local_markets_data:
                    if market_id in market_list:
                        weight = (market_list[market_id]['Visits']) / total_count
                        cost = (market_list[market_id]['Cost']) * weight
                        f_cost += cost
                if f_cost == 0:
                    # print f_cost
                    f_cost = 'Markets in Model NA '
                break
        if f_cost == 0:
            f_cost = 'Quantity too high'
    elif not local_markets_data:
        if farmer_time_cost_model_list:
            f_cost = 'Local markets NA'
        else:
            f_cost = 'Local markets NA and aggregator never entered transport cost'
    # Currently, this condition will be false only when aggregator has never entered any transport cost.
    # But in future, if we want to have a minimum number of transactions before initiating prediction of cost, time,
    # then the condition will hold false in those genuine cases.
    else:
        f_cost = 'Aggregator never entered any transport cost'
    # TODO: How is f_cost still zero?
    if f_cost == 0:
        print '1'
    line_dict_farmer_date['Predicted Cost'] = f_cost
    return line_dict_farmer_date

# Similar/same function for all markets

# Purpose: Identifies local average rate by first identifying weight given to each local market and then rate in those markets.
# It also gives weight to aggregators and local markets. If we we don't know rate for some market, then redistribute weights.
# TODO: If rate of that day is not known, check rates of yesterday / tomorrow.
def append_predicted_rate_in_transaction(line_dict_farmer_date, farmer_time_cost_model_list, local_markets_data, line_dict_dcm):
    average_crop_rate = 0
    total_count = 0.0
    total_quantity = line_dict_farmer_date['Quantity']
    max_quantity_col = 0
    if local_markets_data:
        for keys in farmer_time_cost_model_list.keys():
            max_quantity = keys[max_quantity_col]
            if total_quantity < max_quantity:
                market_list = farmer_time_cost_model_list[keys]
                for market_id in local_markets_data:
                    if market_id in market_list and market_id in line_dict_dcm.keys():
                        total_count += market_list[market_id]['Visits']
                for market_id in local_markets_data:
                    if market_id in market_list and market_id in line_dict_dcm.keys():
                        weight = market_list[market_id]['Visits'] / total_count
                        average_crop_rate += line_dict_dcm[market_id]['Av Rate'] * weight
                if average_crop_rate == 0:
                    average_crop_rate = 'Markets in Model NA or Crop Was Not Sold in Local Markets'
                break
        if average_crop_rate == 0:
            average_crop_rate = 'Quantity Too high'
    else:
        average_crop_rate = 'Local markets NA'
    return average_crop_rate

# Purpose: To read necessary values from different tables and appends them on each transaction level
# TODO: Make aggregator incentive dynamic instead of keeping it fixed at 0.25
def append_impact_in_transaction(line_dict_transaction, line_dict_farmer_date, line_dict_dam, line_dict_damg, line_dict_farmer_time_cost_model, line_local_market, line_dict_dcm):
    quantity = line_dict_transaction['Q']
    rate = line_dict_transaction['R']
    if isinstance(line_dict_farmer_date['Predicted Cost'], float):
        predicted_cpk = line_dict_farmer_date['Predicted Cost'] / line_dict_farmer_date['Quantity']
    else:
        predicted_cpk = line_dict_farmer_date['Predicted Cost']

    if line_dict_damg['CAS'] is None:
        ca_share_pk = 0
    else:
        ca_share_pk = line_dict_damg['CAS'] / line_dict_damg['Q']

    acpk = 0.25
    if isinstance(line_dict_dam['FS'], float):
        fspk = line_dict_dam['FSPK']
        tcpk = line_dict_dam['TCPK']
        digital_green_cost = (tcpk + acpk - fspk - ca_share_pk) * quantity
        if isinstance(predicted_cpk, float):
            profit_transport = (predicted_cpk - fspk)*quantity
        else:
            profit_transport = predicted_cpk
    else:
        fspk = 'Transport Cost Not Entered'
        tcpk = 'Transport Cost Not Entered'
        digital_green_cost = 'Transport Cost Not Entered'
        profit_transport = 'Transport Cost Not Entered'
    average_rate = append_predicted_rate_in_transaction(line_dict_farmer_date, line_dict_farmer_time_cost_model, line_local_market, line_dict_dcm)
    if isinstance(average_rate, float):
        profit_rate = (rate - average_rate) * quantity
    else:
        profit_rate = average_rate

    update_value = (('Predicted CPK', predicted_cpk), ('Average R', average_rate), ('TCPK', tcpk), ('FSPK', fspk),
                    ('CASPK', ca_share_pk), ('ACPK', acpk),('DG Cost', digital_green_cost),
                    ('Profit - Transport', profit_transport), ('Profit - Rate', profit_rate))
    #average_rate_local_market(line_dict_dcm, )
    line_dict_transaction.update(update_value)
    return line_dict_transaction

# --------------------------------------Extra Code----------------------------------------------------------------------
# Function is not doing much right now but it will when calculation of time's model becomes more rigorous
#
# def append_time_in_real_dam(dict_dam, a_m_data):
#     "Reads aggregator-market from real_dam time table and appends time into it"
#     dict_dam['T'] = a_m_data
#     #print dict_dam['T']
#     return dict_dam
