import json, os, sys, re
import pandas as pd
import datetime as dt
from os.path import join as pathjoin
from numpy import mean, std, floor, log10

desired_columns = ['datetime','BLKUID','EXPTIME','FILTER','INSTRUME','OBJECT',
    'OBSTYPE','PROPID','REQNUM','RLEVEL','RA','DEC']

def merge_datasets(dir_path):
    print "Loading Dataframe..."
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
    print "Converting DATE_OBS values to datetime objects..."
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
    mean_ra = mean(ra)
    if mean_ra < 0:
        mean_ra += 360
    return pd.Series((mean_ra, mean(dec)))

def add_ra_dec(df):
    print "Converting Areas to RA and DEC..."
    df[['RA','DEC']] = df['area'].apply(get_centroid)
    return df

def remove_unnecessary_columns(df):
    # Strip all unnecessary column
    global desired_columns
    df = df[ desired_columns ]
    return df

def fill_empty_proposal(input_str):
    if len(input_str) == 0:
        return 'no_proposal'
    else:
        return input_str

def fill_empty_propids(df):
    df['PROPID'] = df['PROPID'].apply(fill_empty_proposal)
    return df

def reduce_frames(df):
    print "Reducing frames..."
    obs_groups = df.groupby('datetime')
    expected_frames = len(obs_groups)

    new_df = pd.concat([
        obs_frames[ obs_frames['RLEVEL'] == obs_frames['RLEVEL'].max() ].head(1)
        for d, obs_frames in obs_groups ]
    )

    resultant_frames = len(new_df)
    if resultant_frames != expected_frames:
        print "Unexpected number of frames returned: {} expected, {} received.".format(\
            expected_frames, resultant_frames)
    return new_df

def contiguous(df):
    blkuid_dict = {}
    contiguous_propid = df.groupby([(df.PROPID != df.PROPID.shift()).cumsum()])
    count = 0
    object_blkuid_split = 0
    for _, g in contiguous_propid:
        for _, g2 in g.groupby('BLKUID'):
            if len(g2.OBJECT.unique()) > 1:
                object_blkuid_split += 1
        # If it's a science frame (attributed to a science proposal)
        science_attempt = False
        for p in g.PROPID.unique():
            if check_science_propid(p):
                science_attempt = True
                break
        if science_attempt:
            if len(g.OBJECT.unique()) > 1:
                if len(g.OBSTYPE.unique()) > 1:
                    print "---"
                    print g
                    count += 1
    print "\nTotal Count:",count
    print "BLKUIDs with different objects:", object_blkuid_split

def contiguous2(df):
    contiguous_propid = df.groupby([(df.PROPID != df.PROPID.shift()).cumsum()])
    for _, g in contiguous_propid:
        # If it's a science frame (attributed to a science proposal)
        science_attempt = False
        for p in g.PROPID.unique():
            if check_science_propid(p):
                science_attempt = True
                break

        blkuid_groups = g.groupby([(df.BLKUID != df.BLKUID.shift()).cumsum()])

        # if science_attempt:
        #     print "---s ({})".format(len(blkuid_groups))
        # else:
        #     print "---c ({})".format(len(blkuid_groups))
        for _, g2 in blkuid_groups:
            # print g2
            # print ""
            return g2

def contiguous3(df):
    contiguous_propid = df.groupby([(df.PROPID != df.PROPID.shift()).cumsum()])
    for _, g in contiguous_propid:
        # If it's a science frame (attributed to a science proposal)
        science_attempt = False
        for p in g.PROPID.unique():
            if check_science_propid(p):
                science_attempt = True
                break

        blkuid_groups = g.groupby([(df.BLKUID != df.BLKUID.shift()).cumsum()])

        for _, sublock in blkuid_groups:
            if science_attempt:
                orphaned = not (('EXPOSE' in sublock.OBSTYPE.unique()) or \
                    ('SPECTRUM' in sublock.OBSTYPE.unique()))
            else:
                orphaned = "-"

            # Get first and last rows
            first_row = sublock.nsmallest(1,'datetime')
            last_row = sublock.nlargest(1,'datetime')
            # Get start date
            start_date = first_row.datetime.iloc[0]
            # Get end date
            end_date = last_row.datetime.iloc[0] + dt.timedelta(
                seconds=float(last_row.EXPTIME.iloc[0]))
            # Get Duration of block
            duration = (end_date - start_date).total_seconds()
            # Get total exposure time
            exposure = sublock.EXPTIME.astype(float).sum()
            # Get exposure efficiency (percent of block where telescope on sky)
            efficiency = round(exposure / duration * 100,2)
            # Get science exposure (percentage of exposures that are science)
            sci_exposure = sublock[ sublock.OBSTYPE.isin(\
                ['EXPOSE','SPECTRUM']
                )].EXPTIME.astype(float).sum()
            sci_efficiency = round(sci_exposure / exposure * 100,2)
            # Get PROPID (Only one, as it has been grouped on)
            propid = sublock.PROPID.unique()[0]
            # Get targets (might be a list. Undesirable, but need to check)
            targets = sublock.OBJECT.unique()
            # Check moving (Determined as RA or DEC changing by > 4 arcseconds
            #   during the block)
            first_ra = first_row.RA.iloc[0]
            last_ra = last_row.RA.iloc[0]
            first_dec = first_row.DEC.iloc[0]
            last_dec = last_row.DEC.iloc[0]
            if (abs(first_ra - last_ra) > 0.001) or \
                (abs(first_dec - last_dec) > 0.001):
                moving = "MOVE"
            else:
                moving = "STAT"
            # Get RA, DEC
            mean_ra = round(sublock.RA.mean(),3)
            mean_dec = round(sublock.DEC.mean(),3)
            # Get text for BOOL variables
            if science_attempt:
                if orphaned:
                    science_text = "SCIENCE (Orphaned)"
                else:
                    science_text = "SCIENCE"
            else:
                science_text = "Calibration"

            print "{}: {} > {} ({}), {}, {}%({}%), {} {} ({}), {}".format(\
                propid, start_date, end_date, duration, targets, efficiency,
                sci_efficiency, mean_ra, mean_dec, moving, science_text)


def check_science_propid(propid):
    return bool(re.match('\w+\d{4}\w-\d+',propid))

if __name__ == '__main__':
    yesno = raw_input('Recompute df? [y/N] ')
    if yesno in ('y','Y'):
        df = merge_datasets('data/lco_data/coj_2m0a_2016-02-01_2016-08-01')
        df = convert_dates_obs(df)
        df = add_ra_dec(df)
        df = remove_unnecessary_columns(df)
        df = reduce_frames(df)
        df = fill_empty_propids(df)
        df = df.sort_values('datetime').reset_index(drop=True)
        contiguous3(df)
