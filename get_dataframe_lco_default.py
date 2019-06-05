import json, os, sys, re
import pandas as pd
import datetime as dt
from os.path import join as pathjoin
from numpy import mean, std, floor, log10, finfo

desired_columns = ['datetime','BLKUID','EXPTIME','FILTER','INSTRUME','OBJECT',
    'OBSTYPE','PROPID','REQNUM','RLEVEL','RA','DEC']

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

def str_to_datetime(date_str):
    try:
        return dt.datetime.strptime(date_str,"%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        return dt.datetime.strptime(date_str,"%Y-%m-%dT%H:%M:%SZ")

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

def fill_empty_proposal(input_str):
    if len(input_str) == 0:
        return 'no_proposal'
    else:
        return input_str

def reduce_frames(df):
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

def is_science_propid(propid):
    return bool(re.match('\w+\d{4}\w-\d+',propid))

def get_largest_intrablock_gap(df):
    # Sort rows based on start time
    # Iterate over rows, get gaps
    # Retain largest gap
    sdf = df.sort_values('datetime')
    largest_gap = 0
    for i in range(len(sdf)-1):
        current_ending = sdf.datetime.iloc[i] + dt.timedelta(\
            seconds=float(sdf.EXPTIME.iloc[i]))
        gap = (sdf.datetime.iloc[i+1] - current_ending).total_seconds()
        if gap > largest_gap:
            largest_gap = gap
    return largest_gap

def get_pattern(block):
    l1 = len(block)
    sblock = block.sort_values('datetime')
    pattern = []
    for row in sblock[['EXPTIME','INSTRUME','FILTER','OBSTYPE']].itertuples(\
        index=False):
        pattern.append( (row[0],row[1],row[2],row[3]) )
    if len(pattern) < l1:
        print "PROBLEM - mismatched lengths"
    return pattern

def condense_pattern(pattern_tuple):
    condensed_list = []
    current_style = pattern_tuple[0]
    current_count = 0
    for frame in pattern_tuple:
        if current_style == frame:
            current_count += 1
        else:
            condensed_list.append( (current_style,current_count) )
            current_style = frame
            current_count = 1
    condensed_list.append( (current_style, current_count) )
    return tuple(condensed_list)

def get_science_proposal_frames(df):
    df2 = pd.DataFrame(df)
    df2['science_propid'] = df2['PROPID'].apply(is_science_propid)
    # df2['science_type'] = df2['OBSTYPE'].isin(['EXPOSE','SPECTRUM'])
    dff = df2.loc[df2['science_propid']]# & df2['science_type']]
    dff.drop('science_propid',axis=1)
    # dff.drop('science_type',axis=1)
    return dff

def extract_target(block):
    if len(block.OBJECT.unique()) == 1:
        return block.OBJECT.unique()[0]
    elif len(block.OBJECT.unique()) == 0:
        return None
    else:
        if all(x in ('SPECTRUM','ARC','LAMPFLAT') for x in block.OBSTYPE):
            if len(block.iloc[2:].OBJECT.unique()) == 1:
                # Due to telescope not moving before initial calibration
                return block.OBJECT.iloc[-1]
            else:
                print "FAILED SUBSET CHECK:"
                print block.iloc[2:]
                print ""
                print block
                print "\n"
        else:
            print "FAILED TYPE CHECK:"
            print block
            print ""

    # If all else fails, return tuple of all targets
    return tuple(sorted(block.OBJECT.unique()))

def extract_science_blocks(all_df):
    # Drop non-science proposals
    df = all_df[ all_df['PROPID'].apply(is_science_propid) ]
    # Extract Blocks
    blkuid_groups = df.groupby('BLKUID')
    block_list = []
    for blkuid, block in blkuid_groups:
        # Get subset of science frames
        block_sci = block[ block['OBSTYPE'].isin(['EXPOSE','SPECTRUM']) ]
        # propid
        propid_list = [ x for x in block.PROPID.unique() ]
        if len(propid_list) > 1:
            print "ERROR: Block with multiple science propids"
            print propid_list
            return None
        propid = propid_list[0]
        # Get the first and last rows of the block
        first_row = block.nsmallest(1,'datetime')
        last_row = block.nlargest(1,'datetime')
        # start_date
        start_date = first_row.datetime.iloc[0]
        # duration
        end_date = last_row.datetime.iloc[0] + dt.timedelta(
            seconds=float(last_row.EXPTIME.iloc[0]))
        duration = (end_date - start_date).total_seconds()
        # exposure_sum
        exposure_sum = block.EXPTIME.astype(float).sum() + finfo(float).eps
            # NOTE: Exclude blocks of Zero duration
        if exposure_sum == 0.0:
            continue
        # science_exposure_sum
        science_exposure_sum = block_sci.EXPTIME.astype(float).sum()
        # time_efficiency
        time_efficiency = round(exposure_sum / duration,5)
        # exposure_science_efficiency
        exposure_science_efficiency = round(science_exposure_sum / exposure_sum,5)
        # total_science_efficiency
        total_science_efficiency = round(science_exposure_sum / duration,5)
        # largest_gap
        largest_gap = get_largest_intrablock_gap(block)
        # targets
        target = extract_target(block)
        # mean_ra and mean_dec
        mean_ra = block.RA.mean()
        mean_dec = block.DEC.mean()
        # moving - Difference of > 4 arcseconds over block
        if (abs(first_row.RA.iloc[0] - last_row.RA.iloc[0]) > 0.001) or \
            (abs(first_row.DEC.iloc[0] - last_row.DEC.iloc[0]) > 0.001):
            moving = True
        else:
            moving = False
        # pattern
        pattern = condense_pattern(get_pattern(block))
        # num_exposures
        num_exposures = len(block)
        # orphan
        if len(block_sci) == 0:
            orphan = True
        else:
            orphan = False
        # reqnum
        reqnum_list = block.REQNUM.unique()
        if len(reqnum_list) > 1:
            reqnum = tuple(sorted(reqnum_list))
        else:
            reqnum = reqnum_list[0]
        # instrument
        instrument_list = block.INSTRUME.unique()
        if len(instrument_list) > 1:
            instrument = tuple(sorted(instrument_list))
        else:
            instrument = instrument_list[0]


        block_list.append({
            'blkuid': blkuid,
            'propid': propid,
            'start_date': start_date,
            'duration': duration,
            'exposure_sum': exposure_sum,
            'science_exposure_sum': science_exposure_sum,
            'time_efficiency': time_efficiency,
            'exposure_science_efficiency': exposure_science_efficiency,
            'total_science_efficiency': total_science_efficiency,
            'largest_gap': largest_gap,
            'target': target,
            'mean_ra': mean_ra,
            'mean_dec': mean_dec,
            'moving': moving,
            'pattern': pattern,
            'orphan': orphan,
            'reqnum': reqnum,
            'instrument': instrument,
            'num_exposures': num_exposures
        })

    return pd.DataFrame(block_list)

def setup():
    print "Loading Dataframe..."
    df = merge_datasets('data/lco_data/coj_2m0a_2016-02-01_2016-08-01')

    print "Converting dates to datetime objects..."
    df['datetime'] = df['DATE_OBS'].apply(str_to_datetime)

    print "Extracting RA and Dec..."
    df[['RA','DEC']] = df['area'].apply(get_centroid)

    print "Dropping excess columns..."
    df = df[ desired_columns ]

    print "Reducing frames..."
    df = reduce_frames(df)

    print "Filling empty proposal IDs..."
    df['PROPID'] = df['PROPID'].apply(fill_empty_proposal)

    print "Sorting frames by date..."
    df = df.sort_values('datetime').reset_index(drop=True)

    print "Extracting science blocks..."
    block_list = extract_science_blocks(df)

    return df, block_list

if __name__ == '__main__':
    df, bl = setup()
