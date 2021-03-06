from datetime import date
from datetime import timedelta

# Enters default value in from_date and to_date
current_date = None
from_date = date(2017,2,1)
to_date = None
no_of_days = 17
time_period = []

# Finds last cycle of 15 days (1-15 or 16-31) w.r.t. given date
def find_last_cycle(assigned_date):
    assigned_year = assigned_date.year
    assigned_month = assigned_date.month
    assigned_day = assigned_date.day

    last_date_of_last_month = assigned_date - timedelta(days=assigned_day)

    # print last_day_of_last_month

    if assigned_day > 15:
        start_date = date(assigned_year, assigned_month, 1)
        end_date = date(assigned_year, assigned_month, 15)
    else:
        start_date = last_date_of_last_month - timedelta(days=last_date_of_last_month.day) + timedelta(days=16)
        end_date = last_date_of_last_month
    time_duration = [start_date, end_date]
    return time_duration

# Insert parameters provided at command prompt in from_date, to_date and no_of_days

current_date = date.today()
if to_date and to_date > current_date:
    print 'To date is greater than today'
elif from_date and from_date > current_date:
    print 'From date is greater than current date'
elif no_of_days and no_of_days < 1:
    print 'No of days should be greater than zero'
elif to_date:
    if from_date is None and no_of_days is None:    # Only to_date - find last cycle before to_date
        print 'Please provide From date or No of days'
    elif from_date and no_of_days is None:          # to_date and from_date - find between this time period
        if from_date < to_date:
            time_period = [from_date, to_date]
        else:
            print 'From date is greater than to date'
    elif from_date is None and no_of_days:          # to_date and no_of_days - find between this time period
        from_date = to_date - timedelta(days= no_of_days - 1)
        time_period = [from_date, to_date]
    else:
        print 'Too many parameters'
else:
    if from_date is None and no_of_days is None:    # No parameters - find last cycle before today
        time_period = find_last_cycle(current_date)
    elif from_date and no_of_days is None:          # from_date - find last cycle from from_date
        print 'Please provide to date or no of days'
    elif from_date is None and no_of_days:          # no_of_days - find between today and today-no_of_days
        time_period = [current_date - timedelta(days = no_of_days - 1), current_date]
    elif from_date and no_of_days:                  # from_date and no_of_days - find between this time period
        if current_date >= from_date + timedelta(days = no_of_days - 1):
            to_date = from_date + timedelta(days = no_of_days - 1)
            time_period = [from_date, to_date]
        else:
            print 'To date is greater than today'
    else:
        print 'It is not possible to reach this else.'

print time_period
