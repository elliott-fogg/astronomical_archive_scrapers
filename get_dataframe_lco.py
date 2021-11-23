import pandas as pd
import json, os, sys
import datetime as dt
from os.path import join as pathjoin
from numpy import mean, std, floor, log10

import plotly.offline as py
import plotly.graph_objs as go

desired_columns = ['datetime','BLKUID','EXPTIME','FILTER','INSTRUME','OBJECT',
    'OBSTYPE','PROPID','REQNUM','RLEVEL','RA','DEC']

def merge_datasets(dir_path):
    print("Loading Dataframe...")
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

def str_to_datetime(date_str):
    try:
        return dt.datetime.strptime(date_str,"%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        return dt.datetime.strptime(date_str,"%Y-%m-%dT%H:%M:%SZ")

def convert_dates_obs(df):
    print("Converting DATE_OBS values to datetime objects...")
    df['datetime'] = df['DATE_OBS'].apply(str_to_datetime)
    return df

def get_centroid(area):
    try:
        coordinates = area.get('coordinates')[0]
    except:
        return pd.Series((None,None))
    if coordinates == None:
        return None
    ra = []
    dec = []
    for corner in coordinates:
        ra.append(corner[0])
        dec.append(corner[1])
    return pd.Series((mean(ra), mean(dec)))

def add_ra_dec(df):
    print("Converting Areas to RA and DEC...")
    df[['RA','DEC']] = df['area'].apply(get_centroid)
    return df

def extract_additional_information(df):
    df = convert_dates_obs(df)
    df = add_ra_dec(df)
    return df

def remove_unnecessary_columns(df):
    # Strip all unnecessary column
    global desired_columns
    df = df[ desired_columns ]
    return df

def reduce_frames(df):
    print("Reducing frames...")
    obs_groups = df.groupby('datetime')
    expected_frames = len(obs_groups)

    new_df = pd.concat([
        obs_frames[ obs_frames['RLEVEL'] == obs_frames['RLEVEL'].max() ].head(1)
        for d, obs_frames in obs_groups ]
    )

    resultant_frames = len(new_df)
    if resultant_frames != expected_frames:
        print("Unexpected number of frames returned: {} expected, {} received.".format(\
            expected_frames, resultant_frames))
    return new_df

def check_groups(df,group_on,check,look_for=None):
    grouper = group_generator(df,group_on)
    while True:
        val, group = next(grouper)
        if len(group) > 1:
            print(group[check].unique())
            if look_for != None:
                print(group.loc[:,look_for])
            x = raw_input("")

def loop_through_requests(df):
    group_by_reqnum = df.groupby('REQNUM')
    print("{} Users Requests in database".format(len(group_by_reqnum)))
    yield

    show_columns = ['id','EXPTIME','OBSTYPE','OBJECT','INSTRUME','FILTER','RLEVEL']
    for reqnum, g_reqnum in group_by_reqnum:
        group_by_start = g_reqnum.groupby('DATE_OBS')
        print("REQUEST: {}, Number of Exposures: {}".format(reqnum,len(group_by_start)))
        exp_number = 0

        for start_time, g_time in group_by_start:
            exp_number += 1
            print("\n\nExposure {}, Starts at {}:\n".format(\
                exp_number, clean_date(start_time)))
            print(g_time.loc[:,show_columns])
            print("")
        yield

def separate_autonomous_requests(df):
    df_grouped = df.groupby('REQNUM')
    for value, group in df_grouped:
        if len(group['PROPID'].unique()) > 1:
            print(value, "-", group['PROPID'].unique())
    print("Unique PropIDs printed.")

def check_obs_structure(df):
    d_c = ['id','DATE_OBS','OBSTYPE','RLEVEL','PROPID']
    df_grouped = df.groupby('DATE_OBS')
    count = 0
    total_count = 0
    for value, group in df_grouped:
        total_count += 1
        no_catalog = group.loc[ group['OBSTYPE'] != 'CATALOG' ]
        no_reduction = no_catalog.loc[ no_catalog['RLEVEL'] == 0 ]
        if len(no_reduction) != 1:
            count += 1
            print(group.loc[:, d_c])
            yield
        # if len(group['OBSTYPE'].unique()) > 1 and 'CATALOG' not in group['OBSTYPE'].unique():
        #     print value, "-", group['OBSTYPE'].unique()
    print("All observation frame groups checked.")
    print("{} / {} groups do not fit expectations".format(count, total_count))

def location_check(df):
    g_requests = df.groupby('REQNUM')
    for value, group in g_requests:
        g_

def get_proposal_data(df):
    # Add in additional information
    df = convert_dates_obs(df)
    df = add_ra_dec(df)
    # Split all information up into proposals
    print("Grouping frames...")
    total_frames = 0
    proposal_groups = df.groupby('PROPID')
    for propid, prop_group in proposal_groups:
        target_requests = prop_group.groupby('OBJECT')
        target_request_numbers = []
        for target, target_request_group in target_requests:
            indv_requests = target_request_group.groupby('REQNUM')
            target_request_numbers.append(len(indv_requests))
            if len(indv_requests) == 0:
                print("0 found!")
                print(target, propid)
                print("")
            total_frames += len(target_request_group)
            for reqnum, request_frames in indv_requests:
                observations = request_frames.groupby('datetime')
        print("{} - {} targets {}".format(propid, len(target_requests), target_request_numbers))







def split_into_user_requests(df):
    proposal_groups = df.groupby('PROPID')
    for propid, prop_group in proposal_groups:
        user_requests = prop_group.groupby('OBJECT')
        for target, user_request_group in user_requests:
            target_requests = []
            indv_requests = user_request_group.groupby('REQNUM')
            for reqnum, request_frames in indv_requests:
                observations = request_frames.groupby('DATE_OBS')
                target_requests.append(len(request_frames))
            print("{} - {}: {}".format(propid, target, target_requests))
        print("")


def obtain_requests(raw_df):
    # Get centroids from visual areas
    # raw_df['centroid'] = raw_df['area'].apply(get_centroid)
    raw_df[['RA','DEC']] = raw_df['area'].apply(get_centroid)
    # Get Datetime object from DATE_OBS
    raw_df['datetime'] = raw_df['DATE_OBS'].apply(str_to_datetime)
    # Strip unnecessary Information
    desired_columns = ['datetime','EXPTIME','FILTER','INSTRUME','OBJECT',
        'OBSTYPE','PROPID','REQNUM','RLEVEL','RA','DEC']
    select_df = raw_df[ desired_columns ]

    proposals = {}

    for reqnum, r_group in select_df.groupby('REQNUM'):
        propid_list = list(r_group['PROPID'].unique())
        if len(propid_list) > 1:
            print("ERROR! - Request group with more than 1 propID")
            return
        propid = propid_list[0]
        observation_list = []

        for start_date, o_group in r_group.groupby('datetime'):
            obs_params = {'DATE_OBS': start_date}
            # Filter out Catalog Observations
            obs_set = o_group[ (o_group['OBSTYPE'] != 'CATALOG') ]
            # Extract identical parameters
            identical_cols = ['EXPTIME','FILTER','INSTRUME','OBJECT','OBSTYPE']
            for key in identical_cols:
                val_list = list(obs_set[key].unique())
                if len(val_list) > 1:
                    print("Unexpected multiple values for '{}' for a single observation".format(key))
                    return
                obs_params[key] = val_list[0]
            # Extract centroid
            obs_params['RA'] = obs_set['RA'].mean(skipna=True)
            obs_params['DEC'] = obs_set['DEC'].mean(skipna=True)

            # Why did I do this? XXX
            # print "RA: {}%, Dec: {}%".format(round(std(ra) / mean(ra) * 100,5),
            #     round(std(dec) / mean(dec) * 100,5))

            observation_list.append(obs_params)
        try:
            proposals[propid].append(observation_list)
        except:
            proposals[propid] = [ observation_list ]

    return proposals


def group_generator(df, key):
    grouped_df = df.groupby(key)
    for value, group in grouped_df:
        yield value, group

def setup():
    df = merge_datasets('data/lco_data/coj_2m0a_2016-02-01_2016-08-01')
    df = extract_additional_information(df)

def plot_data(y_value_dict,x_value_list):
    print("Plotting...")
    bar_data = [
        go.Bar(
            x= x_value_list,
            y=y_value_dict[key],
            name=key
        ) for key in y_value_dict
    ]

    layout = go.Layout(
        barmode='stack',
        xaxis={
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 0.25
        }
    )

    fig = go.Figure(data=bar_data, layout=layout)
    py.plot(fig,filename='plots/stacked-bar.html')

def frame_ra_distribution(df):
    print("Counting RA values...")
    # Convert RA to hours
    # Convert Exposure time to hours
    hour_intervals = 4
    total_bins = int(24 * int(hour_intervals))
    ra_bins_template = [ 0 for i in range(total_bins)]
    proposal_ra = {}
    obs_frames = df.groupby('datetime')
    for time, framelist in obs_frames:
        valid_list = framelist[ framelist['RA'].notna() ]
        try:
            max_rlevel = max(valid_list['RLEVEL'].unique())
        except:
            # print framelist[['RLEVEL','OBSTYPE']]
            # TODO: Filter calibration measurements first
            continue

        row = valid_list.loc[valid_list['RLEVEL'] == max_rlevel]
        ra = row['RA'].iloc[0]
        if ra < 0:
            ra += 360

        exposure = float(row['EXPTIME'].iloc[0])
        proposal_id = row['PROPID'].iloc[0]

        try:
            ra_bin = int(ra / 360. * total_bins)
        except Exception as e:
            print(e)
            print(ra)
            print(type(ra))
            print(row['RA'].iloc[0])
            print("")

        try:
            proposal_ra[proposal_id][ra_bin] += exposure / 3600.
        except:
            proposal_ra[proposal_id] = list(ra_bins_template)
            proposal_ra[proposal_id][ra_bin] += exposure / 3600.

    plot_data(proposal_ra,[ i / hour_intervals for i in range(24*hour_intervals)])

def plot_exposure_times(df):
    print("Counting exposure times...")
    obs_frames = df.groupby('datetime')
    exposure_times = {}
    for time, framelist in obs_frames:
        exptime = framelist['EXPTIME'].unique()[0]
        if float(exptime) == 0.0:
            continue
        try:
            exposure_times[exptime] += 1
        except KeyError:
            exposure_times[exptime] = 1

    # Binning the data
    x_values = [ log10(float(x)) for x in exposure_times.keys() ]
    max_value = int(max(x_values)) + 1
    min_value = int(min(x_values))

    print(max_value, min_value)

    x_bins = [ round(10**(i / 10.),1) for i in range(min_value*10, max_value*10, 1) ]
    y_bins = [ 0 for i in x_bins ]

    for x, y in exposure_times.items():
        x_value = log10(float(x))
        bin = int(floor((x_value - min_value) / 0.1))
        y_bins[bin] += y
    print(x_bins)

    bar_data = [
        go.Bar(
            x=x_bins,
            y=y_bins
        )
    ]

    layout = go.Layout(
        xaxis=dict(
            type='log',
            autorange=True
        )
    )

    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/stacked-bar.html')

def contiguous(df):
    blkuid_dict = {}
    for i, g in df.groupby([(df.BLKUID != df.BLKUID.shift()).cumsum()]):
        blkuid = g.BLKUID.unique()[0]
        try:
            blkuid_dict[blkuid] += 1
        except KeyError:
            blkuid_dict[blkuid] = 1

    for val, num in blkuid_dict.items():
        if num > 1:
            print(val, num)
        else:
            del blkuid_dict[val]

    return blkuid_dict.keys()

def check_contiguous(df,blkuid):
    for i, g in df.groupby([(df.BLKUID != df.BLKUID.shift()).cumsum()]):
        if tuple(g.BLKUID.unique())[0] == blkuid:
            print(g)
            print("")

def verify_contiguous(df,blkuid):
    print(blkuid)
    subset = df[ df.BLKUID == blkuid ]
    print(subset['OBSTYPE'].unique())
    print(subset['PROPID'].unique())

def check_calibration(df,blkuid):
    subset = df[ df.BLKUID == blkuid ]
    obstypes = subset.OBSTYPE.unique()
    if 'EXPOSE' in obstypes or 'SPECTRUM' in obstypes:
        return False
    else:
        return True

def vc(df):
    non_contiguous = contiguous(df)
    interleafed_types = set()
    for blkuid in non_contiguous:
        if not check_calibration(df,blkuid):
            # get_time_diff(df,blkuid)
            # check_contiguous(df,blkuid)
            for t in get_inbetween(df,blkuid,'OBSTYPE'):
                interleafed_types.add(t)
            # print "\n---\n"
        #verify_contiguous(df,blkuid)
    print(interleafed_types)

def get_inbetween(df,blkuid,param):
    subset = df[ df.BLKUID == blkuid ]
    start_time = subset.datetime.min()
    end_time = subset.datetime.max()
    new_subset = df[ (df.datetime >= start_time) & (df.datetime <= end_time) & \
        (df.BLKUID != blkuid)]
    return new_subset[param].unique()

def get_time_diff(df,blkuid):
    subset = df[ df.BLKUID == blkuid ]
    start_time = subset.datetime.min()
    end_time = subset.datetime.max()
    time_diff = end_time - start_time
    print(time_diff)

# def contiguous(df):
#     reqnum_dict = {}
#     for i, g in df.groupby([(df.REQNUM != df.REQNUM.shift()).cumsum()]):
#         reqnum = g.REQNUM.unique()[0]
#         try:
#             reqnum_dict[reqnum] += 1
#         except KeyError:
#             reqnum_dict[reqnum] = 1
#
#     for val, num in reqnum_dict.items():
#         if num > 1:
#             print val, num
#         else:
#             del reqnum_dict[val]
#
#     return reqnum_dict.keys()
#
# def check_contiguous(df,reqnum):
#     for i, g in df.groupby([(df.REQNUM != df.REQNUM.shift()).cumsum()]):
#         if tuple(g.REQNUM.unique())[0] == reqnum:
#             print g
#             print ""
#
# def verify_contiguous(df,reqnum):
#     print reqnum
#     subset = df[ df.REQNUM == reqnum ]
#     print subset['OBSTYPE'].unique()
#     print subset['PROPID'].unique()
#
# def check_calibration(df,reqnum):
#     subset = df[ df.REQNUM == reqnum ]
#     obstypes = subset.OBSTYPE.unique()
#     if 'EXPOSE' in obstypes or 'SPECTRUM' in obstypes:
#         return False
#     else:
#         return True
#
# def vc(df):
#     non_contiguous = contiguous(df)
#     interleafed_types = set()
#     for reqnum in non_contiguous:
#         if not check_calibration(df,reqnum):
#             # get_time_diff(df,reqnum)
#             # check_contiguous(df,reqnum)
#             for t in get_inbetween(df,reqnum,'OBSTYPE'):
#                 interleafed_types.add(t)
#             # print "\n---\n"
#         #verify_contiguous(df,reqnum)
#     print interleafed_types
#
# def get_inbetween(df,reqnum,param):
#     subset = df[ df.REQNUM == reqnum ]
#     start_time = subset.datetime.min()
#     end_time = subset.datetime.max()
#     new_subset = df[ (df.datetime >= start_time) & (df.datetime <= end_time) & \
#         (df.REQNUM != reqnum)]
#     return new_subset[param].unique()
#
# def get_time_diff(df,reqnum):
#     subset = df[ df.REQNUM == reqnum ]
#     start_time = subset.datetime.min()
#     end_time = subset.datetime.max()
#     time_diff = end_time - start_time
#     print time_diff


def extract_blocks(df):
    block_groups = df.groupby('BLKUID')

# XXX: Check whether all BKLUID chunks are contiguous (except for calibration frames)
# i.e. If we look at each block, are there any large gaps in between (e.g. daytime)? 

# TODO:
# Extract scheduling 'blocks' by grouping by BLKUID
# Each block object should contain:
#   Start Time
#   End Time
#   Target
#   Duration (total duration of the block)
#   Total Exposure time (only used telescope time within the block)
#           - maybe this should be only Science Frame time?
#   Efficiency - Total Exposure Time as a fraction of Total Duration
#   Whether or not the block is an 'orphaned' block (no science frames)
#   Whether or not the target is a moving object
#       - classified as the coordinates moving more than 2 arcseconds during a
#           block. Anything less and the block is assumed to be stationary (even
#           if the object is technically moving).
#
# Create a plotly histogram of distributions of block types for each proposal
# Attempt to use distributions to create sample observations for certain
#   proposal types
# See if we can guess the proposal type from the block distribution
#
# Check that the 'area' attribute of frames IS measured in RA and Dec

################################################################################

if __name__ == '__main__':
    df = merge_datasets('data/lco_data/coj_2m0a_2016-02-01_2016-08-01')
    df = extract_additional_information(df)
    df = reduce_frames(df)
    # frame_ra_distribution(df)
    df = remove_unnecessary_columns(df)
    df = df.sort_values('datetime').reset_index(drop=True)
    vc(df)
    # plot_exposure_times(df)


    # proposal_data = get_proposal_data(df)


    # json.dump(proposal_data,open("data/lco_data/coj_2m0a_2016-02-01_2016-08-01/request_data.json","w"))
