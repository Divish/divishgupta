import MySQLdb

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


# {(a, b, c): {d:e, f:g}}
def convert_query_result_in_nested_dictionary(query_result, keys_in_values, key_number):
    final_dictionary = {}
    for tuples in query_result:
        dict_value = []
        temp = ()
        if key_number == 0:
            return None
        if len(keys_in_values) > 0:  # To check whether inner dictionary needs to be provided keys separately
            for elements in keys_in_values:
                temp = ((elements, tuples[keys.index(elements) + key_number]),)
                dict_value += temp
            dict_value = dict(dict_value) # This is inner dictionary
            if key_number == 1:
                dict_key = tuples[0]
            else:
                dict_key = (tuples[0:key_number]) # Creates a tuple which will become key of outer dictionary
        else:
            print 'Please provide some keys.'

        final_dictionary[dict_key] = dict_value
    return final_dictionary

daily_a_m_query = 'SELECT ct.user_created_id, ct.mandi_id, ct.date, u.name_en, m.mandi_name_en, SUM(ct.quantity) AS ' \
                  'Q, dayt.TC,' + '"okay"' +  ',dayt.TC / SUM(ct.quantity) FROM loop_combinedtransaction ct LEFT JOIN ' \
                  'loop_loopuser u ON u.user_id = ct.user_created_id LEFT JOIN loop_mandi m ON m.id = ct.mandi_id ' \
                  'LEFT JOIN (SELECT dt.date D, dt.user_created_id A, dt.mandi_id M, SUM(dt.transportation_cost) TC, ' \
                  'SUM(dt.farmer_share) / COUNT(dt.id) FS FROM loop_daytransportation dt GROUP BY dt.date , ' \
                  'dt.user_created_id , dt.mandi_id) dayt ' \
                  'ON dayt.D = ct.date AND ct.mandi_id = dayt.M AND dayt.A = ct.user_created_id WHERE u.role = 2 GROUP BY ct.date , ' \
                  'ct.user_created_id , ct.mandi_id ORDER BY ct.user_created_id , ct.mandi_id , dayt.TC / SUM(ct.quantity)'
daily_a_m_query_result = onrun_query(daily_a_m_query)

# print daily_a_m_query_result[0]

daily_a_m_farmerShare_query = 'SELECT ct.user_created_id, ct.mandi_id, ct.date, u.name_en, m.mandi_name_en, SUM(ct.quantity) AS ' \
                  'Q, dayt.TC, dayt.FS,  dayt.FS / SUM(ct.quantity), dayt.FS / dayt.TC FROM loop_combinedtransaction ct LEFT JOIN ' \
                  'loop_loopuser u ON u.user_id = ct.user_created_id LEFT JOIN loop_mandi m ON m.id = ct.mandi_id ' \
                  'LEFT JOIN (SELECT dt.date D, dt.user_created_id A, dt.mandi_id M, SUM(dt.transportation_cost) TC, ' \
                  'SUM(dt.farmer_share) / COUNT(dt.id) FS FROM loop_daytransportation dt GROUP BY dt.date , ' \
                  'dt.user_created_id , dt.mandi_id) dayt ' \
                  'ON dayt.D = ct.date AND ct.mandi_id = dayt.M AND dayt.A = ct.user_created_id WHERE u.role = 2 GROUP BY ct.date , ' \
                  'ct.user_created_id , ct.mandi_id ORDER BY ct.user_created_id , ct.mandi_id , ct.date'
daily_a_m_farmerShare_query_result = onrun_query(daily_a_m_farmerShare_query)

a_m_count_query = 'SELECT ct.user_created_id A, ct.mandi_id M, u.name_en, m.mandi_name_en, count(DISTINCT ct.date) ' \
                  'FROM loop_combinedtransaction ct LEFT JOIN loop_loopuser u ON u.user_id = ct.user_created_id ' \
                  'LEFT JOIN loop_mandi m ON m.id = ct.mandi_id WHERE u.role = 2 GROUP BY ct.user_created_id , ct.mandi_id ' \
                  'ORDER BY ct.user_created_id, ct.mandi_id'
a_m_count_query_result = onrun_query(a_m_count_query)

# If I can include them in the SQL Query output, then I won't need to define them separately. But it will then become
#  difficult to use the same query elsewhere.
keys = ('Aggregator', 'Market','C')
a_m_count = convert_query_result_in_nested_dictionary(a_m_count_query_result, keys, 2)

# ----------------------------------Extra Code-----------------------------------------------------------------------

# aggregator_list_query = 'SELECT u.user_id, u.name_en FROM loop_loopuser u'
# aggregator_list_query_result = onrun_query(aggregator_list_query)
#
# aggregator_list = {}
# for lines in aggregator_list_query_result:
#     aggregator_list[lines[0]] = lines[1]

#print len(daily_a_m_query_result)
#print len(a_m_count_query_result)
