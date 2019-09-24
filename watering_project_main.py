

import pandas as pd
import numpy as np
import csv
from datetime import datetime, timedelta
import time
import sys

print(sys.path)
# recording execution time
import thornthwaite
import convert

start = time.time()


def filter_stations(filename: str, sampling_interval: int):
    """
    Filters the input data file and converts into a csv with station property headers

    This function conducts a systemic random sample with specified sampling interval

    :param filename: .txt file of stations to be filtered. Only take datafiles from ghcn
    database that follows standard ghcn
    formatting
    :param sampling_interval: Sampling interval. Can be 1 if you want to convert every station to a csv
    :return: None.
    """
    try:
        fin = open(filename, 'r', buffering=1000000)
    except OSError as e:
        print(e)
        # use stdin to prompt again
    fout_name = f'filtered_stations_{sampling_interval}.csv'
    fout = open(fout_name, 'w', buffering=1000000)
    reader = csv.reader(fin)
    header = ["Station_ID", "Lat", "Long", 'Elev', 'State', 'Name']
    writer = csv.DictWriter(fout, fieldnames=header, delimiter=",")
    writer.writeheader()
    count = 0
    for row in reader:
        count += 1
        row_string = str(row[0])
        if row_string.startswith('US') and count % sampling_interval == 0:
            row_dict = {}
            row_dict["Station_ID"] = row_string[0:11]
            row_dict["Lat"] = row_string[13:21]
            row_dict["Long"] = row_string[22:31]
            row_dict["Elev"] = row_string[32:38]
            row_dict["State"] = row_string[38:40]
            row_dict["Name"] = row_string[41:72]
            writer.writerow(row_dict)

    fin.close()
    fout.close()
    print(count)


def read_data_file_truncate(filename: str, size: int, *args):
    """
    Reads in raw ghcn data files and generates csv of specified size and ghcn data entries of specified types

    Output file name contains original filename, size, and a list of the selected ghcn data types
    :param filename: Ghcn weather data file
    :param size: Number of entries in desired truncated file
    :param args: Types of weather entries desired.
    :return:
    """
    try:
        fin = open(filename, 'r', buffering=1000000)
    except OSError as e:
        print(e)
    arg_string = "_args"
    for index, arg in enumerate(args):
        arg_string = arg_string + "_" + arg
    fout_name = filename[0:len(filename) - 4] + "_" + str(size) + arg_string + ".csv"
    fout = open(fout_name, 'w', buffering=1000000)
    name_list = ["Station_ID", "Date", "Type", "Amount"]
    df = pd.read_csv(fin, delimiter=',', names=name_list, usecols=[0, 1, 2, 3], nrows=size)
    writer = csv.DictWriter(fout, fieldnames=name_list, delimiter=",")
    writer.writeheader()
    for index, row in df.iterrows():
        if (row[2] in args) and (row[3] > 0):
            row_dict = {a: b for a, b in zip(name_list, row[0:4])}
            data_file_helper_list.append(row_dict)
            writer.writerow(row_dict)

    fin.close()
    fout.close()


def read_data_file(filename: str, *args):
    """
    Reads in raw ghcn data files and generates csv of ghcn data entries of specified types

    Output file name contains original filename and a list of the selected ghcn data types
    :param filename: Ghcn weather data file
    :param args: Types of weather entries desired.
    :return:
    """
    try:
        fin = open(filename, 'r', buffering=1000000)
    except OSError as e:
        print(e)
    arg_string = "_args"
    for index, arg in enumerate(args):
        print(arg)
        arg_string = arg_string + "_" + arg
        print(arg_string)
    fout_name = filename[0:len(filename) - 4] + arg_string + ".csv"
    fout = open(fout_name, 'w', buffering=1000000)
    name_list = ["Station_ID", "Date", "Type", "Amount"]
    df = pd.read_csv(fin, delimiter=',', names=name_list, usecols=[0, 1, 2, 3])
    writer = csv.DictWriter(fout, fieldnames=name_list, delimiter=",")
    writer.writeheader()
    for index, row in df.iterrows():
        if (row[2] in args) and (row[3] > 0):
            row_dict = {a: b for a, b in zip(name_list, row[0:4])}
            writer.writerow(row_dict)
            # data_file_helper_list.append(row_dict)
    fin.close()
    fout.close()


def water(df_station_weather_in: pd.DataFrame, start_date: str, end_date: str, interval: int,
          interval_amount: list, default_amount: list, station_in: str):
    """
    Evaluates the water saving based on precipitation data and et_t data for a single station

    :param df_station_weather_in: df containing the weather data for a single station
    :param start_date: day the watering period begins
    :param end_date: day the watering period ends
    :param interval: default number of days between waterings
    :param interval_amount: prescribed amount of water per interval
    :param default_amount: default amount of water per interval
    :param station_in: name of station
    :return: None. appends the total water use and default unadjusted water use to df_water_use
    """
    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    interval = timedelta(days=interval)
    total_use = 0
    baseline_use = 0
    df_index = 0
    surplus = 0
    while start_date < end_date:
        interval_prcp = 0
        df_index_counter = df_index
        for index_local, row_local in df_station_weather_in.iloc[df_index:].iterrows():
            date_prcp = datetime.strptime(str(row_local["Date"]), "%Y%m%d")  # date of the precipitation
            if date_prcp < start_date:
                # print(date_prcp, "was short")
                df_index_counter += 1
            elif start_date <= date_prcp < start_date + interval:  # add the prcp if it's within the date
                interval_prcp += int(row_local["Amount"])
                df_index_counter += 1
            else:  # if the precip is after the date, don't change the search index,
                # interval_prcp is still zero, process as if no rain
                break
        df_index = df_index_counter
        if interval_prcp + surplus >= interval_amount[end_date.month - 1]:
            surplus = min(127, interval_prcp + surplus - interval_amount[end_date.month - 1])
        else:
            total_use += interval_amount[end_date.month - 1] - interval_prcp - surplus
            surplus = 0
        start_date += interval
        baseline_use += default_amount[end_date.month - 1]
    print(f'total_use: {total_use} baseline_use {baseline_use}')
    list_water_use.append({"Station_ID": station_in, "total_use": total_use, "baseline_use": baseline_use})


def calculate_et_t(df_single_station_temp_in: pd.DataFrame, lat_deg: float, year: int, grass_kc: list,
                   station_in: str) -> list:
    """
    Calculates et_t for a given station using the thornthwaite method imported from pyeto

    Requires daily temperature data filtered out of ghcn data, latitude in degrees, year, k_c data as a list per month,
    and string name
    :param df_single_station_temp_in: dataframe containing daily TMAX and daily TMIN
    :param lat_deg: latitude of the station
    :param year: year of the ghcn data
    :param grass_kc: list containing k_c data for each month
    :param station_in: name of the station as a string
    :return: none. appends result to a df
    """
    lat_rad = convert.deg2rad(lat_deg)
    mmdlh = thornthwaite.monthly_mean_daylight_hours(lat_rad, year)
    month_mean_temp_list = []
    month = 1
    month_sum_min = []
    month_sum_max = []
    for index_local, row_local in df_single_station_temp_in.iterrows():
        row_date = datetime.strptime(str(row_local["Date"]), "%Y%m%d")
        row_month = row_date.month
        if month == row_month:
            if row_local["Type"] == "TMIN":
                month_sum_min.append(int(row_local["Amount"]))
            elif row_local["Type"] == "TMAX":
                month_sum_max.append(int(row_local["Amount"]))
        elif row_month == month + 1:
            month_average = (np.mean(month_sum_max) + np.mean(month_sum_min)) / 2
            month_mean_temp_list.append(month_average / 10 * grass_kc[row_month - 1] * scaling_factor / 4)
            month_sum_min = []
            month_sum_max = []
            month += 1
    else:  # cover the 12th month
        month_average = (np.mean(month_sum_max) + np.mean(month_sum_min)) / 2
        month_mean_temp_list.append(month_average / 10 * grass_kc[row_month - 1] * scaling_factor / 4)
    try:
        pet2 = thornthwaite.thornthwaite(month_mean_temp_list, mmdlh, year)
    except ValueError as e:  # necessary in case temperature data is missing months
        print(e)
        return []
    et_t_list2 = []
    for index_local, value in enumerate(pet2):
        et_t_list2.append(int(value))
    print("list of et_t:", et_t_list2)
    print(f'Station {station_in} has complete temp data')
    dict_et_t = {"Station_ID": station_in}
    dict_et_t.update({a: b for a, b in zip([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], et_t_list2)})
    list_et_t.append(dict_et_t)
    return et_t_list2


def station_test(df_single_test: pd.DataFrame, lat_deg: float, kc: list, year: int, start_date: str,
                 end_date: str, interval_length: int, station_str: str):
    """
    Estimates water use for a single station by determining et_t and then calling water()

    Checks for presence of sufficient temperature data to calculate et_t. If sufficient data is present,
    feeds that data into water(). If insufficient temperature data, uses default water demands in water()
    :param df_single_test: dataframe of all weather data for the station
    :param lat_deg: latitude of station in degrees
    :param kc:
    :param year:
    :param start_date:
    :param end_date:
    :param interval_length:
    :param station_str:
    :return:
    """
    # temperature data
    df_single_test_temp = df_single_test.loc[df_single_test["Type"] != "PRCP"]  # not a scalable solution
    print(df_single_test_temp)

    if df_single_test.size > 24:  # screen for if sufficient temperature data to calculate et_t
        print(f'temp data good for station: {station_str}')
        single_station_et_t = calculate_et_t(df_single_test_temp, lat_deg, year, kc, station_str)
        if len(single_station_et_t) > 0:  # if calculate_et_t successful
            et_t_dict = {"Station_ID": station_str}
            et_t_dict.update(  # update to et_t_dict
                {a: b for a, b in zip([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], single_station_et_t)})
            df_single_station_prcp = df_single_test.loc[df_single_test["Type"] == "PRCP"]
            water(df_single_station_prcp, start_date, end_date,
                  interval_length, default_water_demand, default_water_demand,
                  station_str)
        else:  # temperature data insufficient
            print(f'insufficient temp data for station: {station_str}, going solely off prcp values')
            df_single_station_prcp = df_single_test.loc[df_single_test["Type"] == "PRCP"]
            water(df_single_station_prcp, start_date, end_date,
                  interval_length, default_water_demand, default_water_demand,
                  station_str)

    else:  # temperature data insufficient
        print(f'insufficient temp data for station: {station_str}, going solely off prcp values')
        df_single_station_prcp = df_single_test.loc[df_single_test["Type"] == "PRCP"]
        water(df_single_station_prcp, start_date, end_date,
              interval_length, default_water_demand, default_water_demand,
              station_str)
    print(df_water_use)


def test_single_station_search(station_str: str, kc: list, year: int, start_date: str, end_date: str,
                               interval_length: int):
    """
    Evaluates water saving for a specified station by filtering df_data and then calling station_test

    :param station_str: name of station to evaluate
    :param kc: specific crop coefficient. Future versions will accept a list as an input to
    increase granularity
    :param year: Year of the dataset
    :param start_date: first day to begin watering as a YYYYMMDD string
    :param end_date: last day to water as a YYYYMMDD string
    :param interval_length: length of default time between lawn waterings
    :return: None
    """
    station_identifiers = df_stations.loc[df_stations["Station_ID"] == station_str]
    print(station_identifiers)
    lat_deg = station_identifiers["Lat"]
    # generate filtered station data
    df_single_test = df_data.loc[df_data["Station_ID"] == station_str]
    print(df_single_test)
    station_test(df_single_test, lat_deg, kc, year, start_date, end_date,
                 interval_length, station_str)

    print(df_water_use)


def test_all_stations(kc: list, year: int, start_date: str, end_date: str, interval_length: int):
    """
    Tests all stations, iterating through the filtered stations file
    :param kc: list containing monthly K_c data
    :param year: year of ghcn dataset
    :param start_date: start day string in YYYYMMDD
    :param end_date: end day string in YYYYMMDD
    :param interval_length: default length between watering days
    :return:
    """
    for index, row in df_stations.iterrows():
        station_str = row["Station_ID"]
        latitude = row["Lat"]
        df_single_station = df_data.loc[df_data["Station_ID"] == station_str]
        station_test(df_single_station, latitude, kc, year, start_date, end_date,
                     interval_length, station_str)


def process_output(water_use_csv_name="water_use.csv", et_t_csv_name="et_t.csv"):
    """
    Produces printable csv outputs of water use and et_t data, calculates percent saving relative to baseline,
    prints time elapsed since beginning of execution
    :param water_use_csv_name: the desired name of water use data, defaults to "water_use.csv"
    :param et_t_csv_name: desired name of et_t data, defaults ot "et_t.csv"
    :return: execution time elapsed
    """
    df_water_use = pd.DataFrame(list_water_use)
    df_et_t = pd.DataFrame(list_et_t)

    print(df_water_use.head())
    print(df_et_t.head())
    sum_use = df_water_use["total_use"].sum()
    print(f'total use: {sum_use}')
    sum_baseline = df_water_use["baseline_use"].sum()
    print(f'baseline use: {sum_baseline}')
    print("percent saving = ", (sum_baseline - sum_use) / sum_baseline * 100, "%")

    df_water_use.to_csv(water_use_csv_name, sep=',', header=["Station_ID", "total_use", "baseline_use"], index=True)
    df_et_t.to_csv(et_t_csv_name, sep=',', header=["Station_ID", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], index=True)
    end = time.time()
    print("Execution time elapsed: ", time.strftime("%H:%M:%S", time.gmtime(end - start)))
    return ("Execution time elapsed: ", time.strftime("%H:%M:%S", time.gmtime(end - start)))


# df storing water use
df_water_use = pd.DataFrame(columns=["Station_ID", "total_use", "baseline_use"])

# helper list storing water use
list_water_use = []

# df storing et_t data
df_et_t = pd.DataFrame(columns=["Station_ID", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

# helper list storing et_t data
list_et_t = []

# default weekly water use
default_water_demand = [0, 0, 254, 254, 508, 508, 508, 508, 508, 254, 254, 0]

# thornthwaite scaling factor to correct for systemic underestimates
scaling_factor = 4

# kc default
default_kc = [0, 0, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0, 0, 0, 0]

# load/filter station data
# filter_stations("ghcnd-stations-us.txt", 100)
fin_stations = open('filterd_stations_100.csv', 'r', buffering=10000)
df_stations = pd.read_csv(fin_stations, delimiter=',', usecols=[0, 1])

# load/filter weather data
# read_data_file("2017.csv", "PRCP", "TMAX", "TMIN")
fin_data = open("2017_args_PRCP_TMAX_TMIN.csv", 'r', buffering=100000)
df_data = pd.read_csv(fin_data, delimiter=',')

test_single_station_search("ASN00015643", default_kc, 2017, "20170301", "20170831", 7)
test_all_stations(default_kc, 2017, "20170301", "20170831", 7)
process_output()
