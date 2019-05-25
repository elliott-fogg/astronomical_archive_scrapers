import pandas as pd
import json, os, sys
from os.path import join as pathjoin
from numpy import mean

def merge_datasets(dir_path):
    dir_path = pathjoin(dir_path,'working_data')
    datafile_list = filter(lambda x: x.startswith('data'),os.listdir(dir_path))

    df = None

    for datafile in datafile_list:
        filepath = pathjoin(dir_path,datafile)
        data = json.load(open(filepath,"r"))
        new_dataframe = pd.DataFrame(data)

        if type(df) == type(None):
            df = new_dataframe
        else:
            df = pd.concat([df,new_dataframe],axis=0)

    df.reset_index()
    return df

def group_generator(df, key):
    grouped_df = df.groupby(key)
    for value, group in grouped_df:
        yield value, group

def clean_date(date):
    return date.replace('T',' ').replace('Z','')

def get_centroid(area):
    try:
        coordinates = area.get('coordinates')[0]
    except:
        return None
    if coordinates == None:
        return None
    ra = []
    dec = []
    for corner in coordinates:
        ra.append(corner[0])
        dec.append(corner[1])
    return {
        'RA': mean(ra),
        'DEC': mean(dec)
    }

def check_groups(df,group_on,check,look_for=None):
    grouper = group_generator(df,group_on)
    while True:
        val, group = next(grouper)
        if len(group) > 1:
            print group[check].unique()
            if look_for != None:
                print group.loc[:,look_for]
            x = raw_input("")

def loop_through_requests(df):
    group_by_reqnum = df.groupby('REQNUM')
    print "{} Users Requests in database".format(len(group_by_reqnum))
    yield

    show_columns = ['id','EXPTIME','OBSTYPE','OBJECT','INSTRUME','FILTER','RLEVEL']
    for reqnum, g_reqnum in group_by_reqnum:
        group_by_start = g_reqnum.groupby('DATE_OBS')
        print "REQUEST: {}, Number of Exposures: {}".format(reqnum,len(group_by_start))
        exp_number = 0

        for start_time, g_time in group_by_start:
            exp_number += 1
            print "\n\nExposure {}, Starts at {}:\n".format(\
                exp_number, clean_date(start_time))
            print g_time.loc[:,show_columns]
            print ""
        yield

def obtain_requests(raw_df):
    group_by_reqnum = raw_df.groupby('REQNUM')
    print "{} Users Requests in database".format(len(group_by_reqnum))

    desired_colums = ['DATE_OBS','EXPTIME','FILTER','INSTRUME','OBJECT','OBSTYPE',
        'PROPID','REQNUM','RLEVEL','centroid']



################################################################################

if __name__ == '__main__':
    sys.stdout.write('Loading dataframe...')
    sys.stdout.flush()
    df = merge_datasets('data/lco_data/coj_2m0a_2016-02-01_2016-08-01')
    sys.stdout.write('\rDataframe loaded.\n')
    sys.stdout.flush()
    df['centroid'] = df['area'].apply(get_centroid)
