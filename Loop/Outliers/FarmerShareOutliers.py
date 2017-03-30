from datetime import date
import Queries
import Functions

create_workbook = Functions.create_workbook
create_xlsx = Functions.create_xlsx
create_format = Functions.create_format

time_period = [date(2015,6,1), date.today()]

# List ordered by aggregator, market, date
daily_a_m_farmerShare = Queries.daily_a_m_farmerShare_query_result
aggregator_id_col = 0
market_col = 1
date_col = 2
aggregator_name_col = 3
FSPK_col = 8
FSPTC_col = 9


# Dictionary {(aggregator id, market id): count of visits}
a_m_count = Queries.a_m_count

# initialisation
start_point = 0
count = 0
aggregator_id = daily_a_m_farmerShare[0][aggregator_id_col]
aggregator_name = daily_a_m_farmerShare[0][aggregator_name_col]
market_id = daily_a_m_farmerShare[0][market_col]
last_FSPK = daily_a_m_farmerShare[0][FSPK_col]
last_FSPTC = daily_a_m_farmerShare[0][FSPTC_col]
no_of_rows = a_m_count[(aggregator_id, market_id)]['C']

aggregator_wise_FShare_outliers = {}
daily_a_m_farmerShare_filtered = []

from_date = time_period[0]
to_date = time_period[1]

for line in daily_a_m_farmerShare:
    if line[date_col] >= from_date and line[date_col] <= to_date:
        daily_a_m_farmerShare_filtered.append(line)

for line in daily_a_m_farmerShare_filtered:
    FSPK = line[FSPK_col]
    FSPTC = line[FSPTC_col]
    if count < no_of_rows + start_point:
        if FSPK != last_FSPK and FSPTC != last_FSPTC:
            if aggregator_name in aggregator_wise_FShare_outliers.keys():  # Check: Aggregator ID exists
                aggregator_wise_FShare_outliers[aggregator_name].append(line[2:])
                aggregator_wise_FShare_outliers['All'].append(line[2:])
            else:
                aggregator_wise_FShare_outliers[aggregator_name] = [line[2:]]
                if 'All' in aggregator_wise_FShare_outliers.keys():
                    aggregator_wise_FShare_outliers['All'].append(line[2:])
                else:
                    aggregator_wise_FShare_outliers['All'] = [line[2:]]
    else:
        aggregator_id = line[aggregator_id_col]
        aggregator_name = line[aggregator_name_col]
        market_id = line[market_col]
        start_point += no_of_rows
        no_of_rows = a_m_count[(aggregator_id, market_id)]['C']
    count += 1
    last_FSPK = FSPK
    last_FSPTC = FSPTC

# Position of All is first as a co-incidence I think.
# Set correct column widths
workbook = create_workbook('Farmer Share Outliers.xlsx')
all_format = ['date_format']
all_format_created = create_format(all_format, workbook)
column_properties = [{'header': 'Date', 'format': all_format_created['date_format']}, {'header': 'Aggregator'}, {'header': 'Market'}, {'header': 'Quantity'}, {'header': 'TCost'}, {'header': 'Farmer Share'}, {'header': 'FSPK'}, {'header': 'FSPTC'}]
table_properties = {'data': None, 'autofilter': False, 'banded_rows': False, 'style': 'Table Style Light 15', 'columns': column_properties}
column_width = {'A:A': 10.55, 'B:B': 9.36}
create_xlsx(workbook, aggregator_wise_FShare_outliers, table_properties, column_width_and_format = column_width)

