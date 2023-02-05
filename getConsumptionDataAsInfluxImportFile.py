import getopt
import os

import os
from datetime import date, datetime, timedelta
import sys
from pycaruna import CarunaPlus, TimeSpan
import datetime as dt
import time

from pycaruna.authenticator import Authenticator


def make_min_hour_datetime(date):
    return datetime.combine(date, datetime.min.time())


def make_max_hour_datetime(date):
    return datetime.combine(date, datetime.max.time()).replace(microsecond=0)

def main(argv):
    timespan = 'hours'
    dateArg = ''
    opts, args = getopt.getopt(argv,"ht:d:",["timespan=", "date="])
    for opt, arg in opts:
        if opt == '-h':
            print ('getConsumptionDataAsInfluxImportFile.py -t <hours/days/months>')
            sys.exit()
        elif opt in ("-t", "--timespan"):
            timespan = arg
        elif opt in ("-d", "--date"):
            dateArg = arg
    username = os.getenv('CARUNA_USERNAME')
    password = os.getenv('CARUNA_PASSWORD')
    

    if username is None or password is None:
        raise Exception('CARUNA_USERNAME and CARUNA_PASSWORD must be defined')

    authenticator = Authenticator(username, password)
    login_result = authenticator.login()
    token = login_result['token']
    customer_id = login_result['user']['ownCustomerNumbers'][0]

    if token is None or customer_id is None:
        raise Exception('Token and Customer_id must be defined')

    # Create a Caruna Plus client
    client = CarunaPlus(token)

    # Get customer details and metering points so we can get the required identifiers
    customer = client.get_user_profile(customer_id)
    # print(customer)

    # Get metering points, also known as "assets". Each asset has an "assetId" which is needed e.g. to
    # retrieve energy consumption information for a metering point type asset.
    metering_points = client.get_assets(customer_id)
    # print(metering_points)

    asset_id = metering_points[0]['assetId']
    start_date = date.today() if not dateArg else datetime.strptime(dateArg, '%d-%m-%Y')
    end_date = make_min_hour_datetime(date.today())
     # Get daily usage for the month of January 2023 for the first metering point. Yes, this means TimeSpan.MONTHLY. If
    # you want hourly usage, use TimeSpan.DAILY.
    delta = timedelta(days=1)
    match timespan:
        case 'hours':
            caruna_timespan = TimeSpan.DAILY
        case 'days':
            caruna_timespan = TimeSpan.MONTHLY
            start_date = start_date.replace(day=1, hour=0, minute=0, second=0)
        case 'months':
            caruna_timespan = TimeSpan.YEARLY
        case _:
            raise Exception('Could not convert command line argument timestamp to Caruna timestamp. Timestamp: '+timespan)
    print("# DML")
    print("# CONTEXT-DATABASE: electricity")

    while start_date <= end_date:
        consumption = client.get_energy(customer_id, asset_id, caruna_timespan, start_date.year, start_date.month, start_date.day)
        if timespan == 'months':
            start_date = start_date.replace(year=start_date.year+1)
        elif timespan == 'days':
            start_date = start_date.replace(month=start_date.month+1) 
        else: 
            start_date += delta
        filtered_consumption = [x for x in consumption if 'totalConsumption' in x]
        # Extract the relevant data, filter out days without values (usually the most recent datapoint)
        mapped_consumption = list(map(lambda item: {
            'metering_point':  metering_points[0]['id'],
            'timestamp': (int)(time.mktime(datetime.fromisoformat(item['timestamp']).timetuple())),
            'kwh_total': item['totalConsumption'],
            'timespan': timespan
        }, filtered_consumption))

        influx_input = map(
            lambda item: "electricity_consumption,metering_company=Caruna,metering_point=%s,timespan=%s kwh_total=%s %s"
                %(
                item['metering_point'],
                item['timespan'],
                item['kwh_total'],
                item['timestamp']
                )
            , mapped_consumption)

        [print(i) for i in influx_input]

if __name__ == '__main__':
    main(sys.argv[1:])

