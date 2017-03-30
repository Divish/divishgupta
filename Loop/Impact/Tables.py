import MySQLdb
import Functions

mysql_cn = MySQLdb.connect(host='localhost', port=3306, user='root',
                           passwd='root',
                           db='digitalgreen_local',
                           charset='utf8',
                           use_unicode=True)

def onrun_query(query):
    cursor = mysql_cn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Purpose: Update dictionary.
update_dictionary = Functions.update_dictionary

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

# TODO: Use this to create a filter.
aggregator_local_market_data = {2286: [1, 3], 2287: [1, 3], 2584: [1, 3], 2749: [1, 7], 2748: [4, 7], 2585: [3],
                                2948: [3, 8], 2953: [1], 2586: [3, 12], 2946: [12]}


# Purpose: To store Aggregator-CA level information. It'll be used to predict sustainability if this CA was chosen
# GSPK should be based on commission + outliers from last change in commission.
# This means that I don't need this query which I wrote after putting in so much effort!
aggregator_ca_data_query = "SELECT Ctrans.Agg AS A, Ctrans.Man AS M, Ctrans.CAgent AS CA, SUM(CTrans.Quant), " \
                           "SUM(G.Total_Discount), SUM(G.Total_Discount)/SUM(CTrans.Quant) " \
                           "FROM (SELECT ct.date AS D, ct.user_created_id AS Agg, ct.mandi_id AS Man, " \
                           "ct.gaddidar_id AS CAgent, SUM(ct.quantity) AS Quant FROM loop_combinedtransaction ct " \
                           "GROUP BY ct.date , ct.user_created_id , ct.mandi_id , ct.gaddidar_id) Ctrans " \
                           "LEFT JOIN (SELECT lct.date AS date, lct.user_created_id AS aggregator, " \
                           "lct.gaddidar_id AS gaddidar_id, lct.mandi_id AS mandi_id, SUM(lct.quantity) AS quantity, " \
                           "SUM(lct.amount) AS amount, gc.start_date AS start_date, gc.discount_percent, lg.discount_criteria," \
                           "IF(lg.discount_criteria = 0, gc.discount_percent * SUM(lct.quantity), gc.discount_percent * SUM(lct.amount)) AS Total_Discount " \
                           "FROM loop_combinedtransaction lct " \
                           "LEFT JOIN (SELECT lgc.start_date, lgc.gaddidar_id, lgc.discount_percent " \
                           "FROM loop_gaddidarcommission lgc ORDER BY lgc.start_date) gc ON lct.date >= gc.start_date AND lct.gaddidar_id = gc.gaddidar_id " \
                           "LEFT JOIN loop_gaddidar lg ON lg.id = lct.gaddidar_id GROUP BY lct.date , lct.user_created_id , lct.gaddidar_id) G " \
                           "ON G.aggregator = Ctrans.Agg AND G.gaddidar_id = Ctrans.CAgent AND G.mandi_id = Ctrans.Man AND G.date = CTrans.D " \
                           "GROUP BY Ctrans.Agg , Ctrans.CAgent"

aggregator_ca_data_query_result = onrun_query(aggregator_ca_data_query)

aggregator_ca_data = {}
for lines in aggregator_ca_data_query_result:
    aggregator_id = lines[0]
    market_id = lines[1]
    ca_id = lines[2]
    quantity = lines[3]
    ca_share = lines[4]
    ca_share_pk = lines[5]
    update_value = {(aggregator_id, market_id, ca_id): {'Q': quantity, 'CA Share': ca_share, 'CASPK': ca_share_pk}}
    update_dictionary(aggregator_ca_data, update_value)


# Purpose: To story daily quantity given by farmers. Predicted cost and time for transactions will depend on this.
# TODO: Won't work if a farmer gives produce to multiple aggregators in a day. Fix.
# Initially, it will only have Q in values. Predicted cost and time will be appended.
farmer_predict_daily_cost_time_query = 'SELECT date, user_created_id, farmer_id, SUM(quantity)FROM loop_combinedtransaction GROUP BY date , ' \
                               'user_created_id, farmer_id '

farmer_predict_daily_cost_time_query_result = onrun_query(farmer_predict_daily_cost_time_query)
daily_farmer_predicted_cost_time_data = {}
for lines in farmer_predict_daily_cost_time_query_result:
    date = lines[0]
    aggregator_id = lines[1]
    farmer_id = lines[2]
    quantity = lines[3]
    update_value = {(date, aggregator_id, farmer_id): {'Quantity': quantity}}
    update_dictionary(daily_farmer_predicted_cost_time_data, update_value)

# Purpose: To get TCPK and FSPK for each D-A-M combination which will be later mapped to each transaction.
# Assumption: TC, AC and FS are uniform for every kg of a D-A-M combination. FS might depend on CA in future.
# Quantity isn't necessary. Just a temp variable to get others.
# ACPK is not added anywhere right now.
daily_aggregator_market_query = 'SELECT ct.date, ct.user_created_id, ct.mandi_id, SUM(ct.quantity) AS Q, dayt.TC, dayt.TC / SUM(' \
                  'ct.quantity), dayt.FS, dayt.FS / SUM(ct.quantity) FROM loop_combinedtransaction ct LEFT JOIN (SELECT dt.date D, ' \
                  'dt.user_created_id A, dt.mandi_id M, SUM(dt.transportation_cost) TC, SUM(dt.farmer_share) / COUNT(' \
                  'dt.id) FS, COUNT(dt.id) Vehicle_Count FROM loop_daytransportation dt GROUP BY dt.date , dt.user_created_id , dt.mandi_id) dayt ' \
                  'ON dayt.D = ct.date AND ct.mandi_id = dayt.M AND dayt.A = ct.user_created_id GROUP BY ct.date , ' \
                  'ct.user_created_id , ct.mandi_id'

daily_aggregator_market_query_result = onrun_query(daily_aggregator_market_query)
daily_aggregator_market_data = {}

for lines in daily_aggregator_market_query_result:
    date = lines[0]
    aggregator_id = lines[1]
    market_id = lines[2]
    quantity = lines[3]
    transport_cost = lines[4]
    transport_cpk = lines[5]
    farmer_share = lines[6]
    farmer_share_pk = lines[7]
    update_value = {(date, aggregator_id, market_id): {'Q': quantity, 'TC': transport_cost, 'TCPK': transport_cpk, 'FS': farmer_share, 'FSPK':farmer_share_pk}}
    update_dictionary(daily_aggregator_market_data, update_value)


# Purpose: To get GSPK for each D-A-M-G combination which will be later mapped to each transaction.
# Assumption: GS is uniform for every kg of D-A-G combination.
daily_aggregator_market_ca_query = "SELECT lct.date AS date, lct.user_created_id AS aggregator, lct.mandi_id AS mandi_id, " \
                    "lct.gaddidar_id AS gaddidar_id, SUM(lct.quantity) AS quantity, SUM(lct.amount) AS amount, " \
                    "IF(lg.discount_criteria = 0, gc.discount_percent * SUM(lct.quantity), gc.discount_percent * " \
                    "SUM(lct.amount)) AS Total_Discount " \
                    "FROM loop_combinedtransaction lct LEFT JOIN " \
                    "(SELECT lgc.start_date, lgc.gaddidar_id, lgc.discount_percent " \
                    "FROM loop_gaddidarcommission lgc ORDER BY lgc.start_date) gc " \
                    "ON lct.date >= gc.start_date AND lct.gaddidar_id = gc.gaddidar_id " \
                    "LEFT JOIN loop_gaddidar lg ON lg.id = lct.gaddidar_id " \
                    "GROUP BY lct.date , lct.user_created_id , lct.mandi_id, lct.gaddidar_id"
daily_aggregator_market_ca_query_result = onrun_query(daily_aggregator_market_ca_query)
daily_aggregator_market_ca_data = {}

for lines in daily_aggregator_market_ca_query_result:
    date = lines[0]
    aggregator_id = lines[1]
    market_id = lines[2]
    ca_id = lines[3]
    quantity = lines[4]
    amount = lines[5]
    ca_share = lines[6]
    update_value = {(date, aggregator_id, market_id, ca_id): {'Q': quantity, 'A': amount, 'CAS': ca_share}}
    update_dictionary(daily_aggregator_market_ca_data, update_value)

#Replaces gaddidar outliers in daily_aggregator_market_ca_data with values from outliers table
daily_aggregator_market_ca_outliers_query = "SELECT gso.date, u.user_id, gso.mandi_id, gso.gaddidar_id, gso.amount " \
                             "FROM loop_gaddidarshareoutliers gso LEFT JOIN loop_loopuser u ON u.id = gso.aggregator_id"
daily_aggregator_market_ca_outliers_query_result = onrun_query(daily_aggregator_market_ca_outliers_query)
daily_aggregator_market_ca_outliers_data = {}

for lines in daily_aggregator_market_ca_outliers_query_result:
    date = lines[0]
    aggregator_id = lines[1]
    market_id = lines[2]
    ca_id = lines[3]
    ca_share = lines[4]
    update_value = {(date, aggregator_id, market_id, ca_id): {'CAS': ca_share}}
    update_dictionary(daily_aggregator_market_ca_outliers_data, update_value)

for keys in daily_aggregator_market_ca_outliers_data.keys():
    daily_aggregator_market_ca_data[keys]['CAS'] = daily_aggregator_market_ca_outliers_data[keys]['CAS']

# Purpose: To get daily average rate for each crop mandi wise. To identify the best market.
# Deviation across days and within a day could also be decision-determining parameters for identifying the best market

daily_crop_market_query = 'SELECT date, crop_id, mandi_id, SUM(quantity), SUM(amount) / SUM(quantity) FROM loop_combinedtransaction GROUP BY ' \
                          'date , crop_id , mandi_id '
daily_crop_market_query_result = onrun_query(daily_crop_market_query)
daily_crop_market_data = {}

for lines in daily_crop_market_query_result:
    date = lines[0]
    crop_id = lines[1]
    market_id = lines[2]
    quantity = lines[3]
    average_rate = lines[4]
    update_value = {(date, crop_id): {market_id: {'Q': quantity, 'Av Rate': average_rate}}}
    update_dictionary(daily_crop_market_data, update_value)

# Purpose: To have data for each transaction. Every data (ACPK, TCPK, FSPK, Predicted...) will be mapped on this level.
transaction_level_data_query = 'SELECT date, user_created_id, mandi_id, gaddidar_id, farmer_id, crop_id, quantity, price ' \
                               'FROM loop_combinedtransaction'
transaction_level_data_query_result = onrun_query(transaction_level_data_query)
transaction_level_data = []

for lines in transaction_level_data_query_result:
    date = lines[0]
    aggregator_id = lines[1]
    market_id = lines[2]
    ca_id = lines[3]
    farmer_id = lines[4]
    crop_id = lines[5]
    quantity = lines[6]
    rate = lines[7]
    update_value = {'D': date, 'A': aggregator_id, 'M': market_id, 'CA': ca_id, 'F': farmer_id, 'C': crop_id, 'Q': quantity, 'R': rate }
    transaction_level_data.append(update_value)
mysql_cn.close()

#___________________________________________________________Extra Code__________________________________________________

# # Updated Purpose: I think we will never use it to predict cost of aggregator. We might use something similar to
# # predict farmer share.
# # Purpose: This table contains Aggregator-Market level information which would be used to predict cost, time if
# # aggregator would have gone to this data.
# # Will we only store recent data or all data?
# # Currently, we are storing all data but capturing recent data might be more accurate.
# aggregator_market_data_query = 'SELECT ct.user_created_id, ct.mandi_id, SUM(ct.quantity) AS Q, dayt.TC, dayt.TC / ' \
#                                'SUM(ct.quantity), dayt.FS / SUM(ct.quantity) FROM loop_combinedtransaction ct LEFT ' \
#                                'JOIN  (SELECT dt.user_created_id A, dt.mandi_id M, SUM(dt.transportation_cost) TC, ' \
#                                'SUM(dt.farmer_share) / COUNT(dt.id) FS FROM loop_daytransportation dt GROUP BY ' \
#                                'dt.user_created_id , dt.mandi_id) dayt ON ct.mandi_id = dayt.M AND dayt.A = ' \
#                                'ct.user_created_id GROUP BY ct.user_created_id , ct.mandi_id '
# aggregator_market_data_query_result = onrun_query(aggregator_market_data_query)
# keys = ('Q', 'TC', 'TCPK', 'FSPK')
#
# aggregator_market_data = convert_query_result_in_nested_dictionary(aggregator_market_data_query_result, keys, 2)
