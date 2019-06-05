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
            if is_science_propid(p):
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
            if is_science_propid(p):
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
            if is_science_propid(p):
                science_attempt = True
                break

        blkuid_groups = g.groupby([(df.BLKUID != df.BLKUID.shift()).cumsum()])

        for _, sublock in blkuid_groups:
            if science_attempt:
                orphan = not (('EXPOSE' in sublock.OBSTYPE.unique()) or \
                    ('SPECTRUM' in sublock.OBSTYPE.unique()))
            else:
                orphan = "-"

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
                if orphan:
                    science_text = "SCIENCE (orphan)"
                else:
                    science_text = "SCIENCE"
            else:
                science_text = "Calibration"

            print "{}: {} > {} ({}), {}, {}%({}%), {} {} ({}), {}".format(\
                propid, start_date, end_date, duration, targets, efficiency,
                sci_efficiency, mean_ra, mean_dec, moving, science_text)

def is_science_propid(propid):
    return bool(re.match('\w+\d{4}\w-\d+',propid))

def extract_blocks(df):
    # Group by BLKUID
    # Discard BLKUIDs referring to calibration-only blocks
    # Get start date, end date of remaining blocks
    # Chunk observations on these lines
    # Group remaining observations as calibration blocks
    # XXX: Check that non-calibration blocks do not intersect CONFIRMED

    # Separate Science and Calibration BLKUIDs
    blkuid_groups = df.groupby('BLKUID')
    science_blkuids = []
    calibration_blkuids = []
    for blkuid, group in blkuid_groups:
        # science_type = False
        # for obstype in ['EXPOSE','SPECTRUM']:
        #     if obstype in group['OBSTYPE'].unique():
        #         science_type = True
        #         break
        # if science_type:
        science_proposal = False
        for propid in group['PROPID'].unique():
            if is_science_propid(propid):
                science_proposal = True
                break

        if science_proposal:
            science_blkuids.append(blkuid)
        else:
            calibration_blkuids.append(blkuid)

    # Check science blocks do not intersect
    block_info = []
    print len(science_blkuids), len(calibration_blkuids)
    for sci_blkuid in science_blkuids:
        subframe = df[ df['BLKUID'] == sci_blkuid ]

        first_row = subframe.nsmallest(1,'datetime')
        last_row = subframe.nlargest(1,'datetime')

        start_date = first_row.datetime.iloc[0]
        # Get end date
        end_date = last_row.datetime.iloc[0] + dt.timedelta(
            seconds=float(last_row.EXPTIME.iloc[0]))

        block_info.append( [sci_blkuid, start_date, end_date] )
    df2 = pd.DataFrame(block_info,columns=['blkuid','start','end'])
    df3 = df2.sort_values('start').reset_index(drop=True)
    for i in range(len(df3)):
        previous_end_date = df3['end'].iloc[0]
        df3 = df3.shift(-1)
        if previous_end_date > df3['start'].min():
            print "ERROR!!"
            print "Previous end date: {}".format(previous_end_date)
            print "Current earliest start: {}".format(df3['start'].min())
    print "complete"

    blockframe_list = []
    # print df2.head()

    for start_time, end_time in df2[['start','end']].itertuples(index=False):
        blockframe = df[ (df['datetime'] >= start_time) & \
            (df['datetime'] < end_time) ]
        # print blockframe
        # print ""
        blockframe_list.append(blockframe)

    return blockframe_list

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

def get_pattern(df):
    l1 = len(df)
    sdf = df.sort_values('datetime')
    pattern = []
    for row in sdf[['EXPTIME','INSTRUME','FILTER','OBSTYPE']].itertuples(index=False):
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

def condense_blocks(blockframe_list):
    block_list = []
    gap_list = []
    for blockframe in blockframe_list:
        propid_list = [ x for x in blockframe.PROPID.unique() if is_science_propid(x) ]
        if len(propid_list) > 1:
            print "ERROR: Block with multiple science propids"
            print propid_list
            return None
        propid = propid_list[0]
        science_frames = get_science_proposal_frames(blockframe)
        start_date = blockframe.datetime.min()
        end_row = blockframe.nlargest(1,'datetime')
        end_date = end_row.datetime.iloc[0] + dt.timedelta(
            seconds=float(end_row.EXPTIME.iloc[0]))
        duration = (end_date - start_date).total_seconds()
        if duration == 0.0:
            print "Zero Block Duration"
            print blockframe
            continue
        largest_gap = get_largest_intrablock_gap(blockframe)
        gap_list.append(largest_gap)
        target_list = blockframe.OBJECT.unique()
        exposure_duration = blockframe['EXPTIME'].astype(float).sum()
        science_duration = science_frames['EXPTIME'].astype(float).sum()
        total_efficiency = float(exposure_duration) / float(duration)
        exposure_science_efficiency = float(science_duration) / (float(exposure_duration) + 0.0001)
        total_science_efficiency = float(science_duration) / float(duration)
        mean_ra = round(blockframe.RA.mean(),3)
        mean_dec = round(blockframe.DEC.mean(),3)
        if len(science_frames) > 0:
            # Is Science Frame
            first_ra = science_frames.nsmallest(1,'datetime').RA.iloc[0]
            last_ra = science_frames.nlargest(1,'datetime').RA.iloc[0]
            first_dec = science_frames.nsmallest(1,'datetime').DEC.iloc[0]
            last_dec = science_frames.nlargest(1,'datetime').DEC.iloc[0]
            if (abs(first_ra - last_ra) > 0.001) or \
                (abs(first_dec - last_dec) > 0.001):
                moving = True
            else:
                moving = False
            pattern = condense_pattern(get_pattern(science_frames))
            orphan = True
            for sci_type in ['EXPOSE','SPECTRUM']:
                if sci_type in science_frames['OBSTYPE'].unique():
                    orphan = False
                    break
        else:
            moving = None
            orphan = None
            pattern = None

        block_list.append({
            'PROPID': propid,
            'start_date': start_date,
            'duration': duration,
            'largest_gap': largest_gap,
            'targets': target_list,
            'exposure_efficiency': total_efficiency,
            'science_effiency': exposure_science_efficiency,
            'total_efficiency': total_science_efficiency,
            'ra': mean_ra,
            'dec': mean_dec,
            'moving': moving,
            'orphan': orphan,
            'pattern': pattern
        })
    return pd.DataFrame(block_list)
    # return pd.DataFrame(block_list,columns=['PROPID','start_date','duration',
    #     'largest_gap','targets','exposure_efficiency','science_effiency',
    #     'total_efficiency','ra','dec','moving','orphan','pattern'])

def extract_patterns(blockframe_list):
    patterns = {}
    for blockframe in blockframe_list:
        blockframe = get_science_proposal_frames(blockframe)
        if len(blockframe) == 0:
            print "TRIGGERED!!!!!!!!!!!"
            continue
        p = get_pattern(blockframe)
        cp = condense_pattern(p)
        if len(p) == 0:
            print "No pattern detected"
            print blockframe
        elif len(cp) == 0:
            print 'Patterns lost on condensing'
            print blockframe
        try:
            patterns[cp] += 1
        except KeyError:
            patterns[cp] = 1
    return patterns

def print_patterns(pattern_dict):
    pattern_list = list(pattern_dict.items())
    pattern_list.sort(key=lambda x: x[1],reverse=True)
    for entry in pattern_list:
        print entry[1]
        print entry[0]
        print ""

if __name__ == '__main__':
    # yesno = raw_input('Recompute df? [Y/n] ')
    # if yesno in ('y','Y',""):
    print "\n{}\n".format("MERGING DATASETS")
    df = merge_datasets('data/lco_data/coj_2m0a_2016-02-01_2016-08-01')
    print "\n{}\n".format("CONVERTING DATES")
    df = convert_dates_obs(df)
    print "\n{}\n".format("ADDING RA AND DEC")
    df = add_ra_dec(df)
    print "\n{}\n".format("REMOVING UNNECESSARY COLUMNS")
    df = remove_unnecessary_columns(df)
    print "\n{}\n".format("REDUCING FRAMES")
    df = reduce_frames(df)
    print "\n{}\n".format("FILLING EMPTY PROPIDS")
    df = fill_empty_propids(df)
    print "\n{}\n".format("SORTING VALUES")
    df = df.sort_values('datetime').reset_index(drop=True)
    print "\n{}\n".format("EXTRACTING BLOCKS")
    blockframe_list = extract_blocks(df)
    print "\n{}\n".format("CONDENSING BLOCKS")
    block_list = condense_blocks(blockframe_list)
        # print "patterns"
        # patterns = extract_patterns(blockframe_list)
        # print "print patterns"
        # print_patterns(patterns)
