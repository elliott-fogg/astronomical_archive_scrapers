# A series of functions for Exploratory Data Analysis of the LCO data, to check
# it is consistent in ways that I expect and to play around with new hypothesese
# without having to remember everything I did in IPython.

from get_dataframe_lco import *

def check_reqnums(df):
    # Check that no simultaneous requests have different REQNUMs
    print "Checking if all simultaneous frames belong to same request..."
    date_grouper = group_generator(df, 'DATE_OBS')
    triggered = False
    trig_cases = []
    while True:
        try:
            val, group = next(date_grouper)
            if len(group) > 1:
                if len(group['REQNUM'].unique()) > 1:
                    triggered = True
                    trig_cases.append( [val, list(group['REQNUM'].unique())] )
        except StopIteration:
            break
    if triggered:
        print "WARNING: Some frames at identical times have different REQNUMs"
        for case in trig_cases:
            print case
    else:
        print "Success: All simultaneous frames are of the same Requests"

def check_request_homogeneity(df):
    # Check that all frames in each request are from the same proposal?
    print "Checking homogeneity within requests..."
    params_to_check = ['PROPID','OBJECT','INSTRUME'] # area?

    req_grouper = group_generator(df,'REQNUM')
    trigger_cases = {}
    multiple_target_type_set = set()
    while True:
        try:
            val, group = next(req_grouper)
            if len(group) > 1:
                for param in params_to_check:
                    if len(group[param].unique()) > 1:
                        print group[param].unique()

                        print group['OBSTYPE'].unique()
                        type_uniques = group['OBSTYPE'].unique()
                        sorted_uniques = sorted(list(type_uniques))
                        tuple_uniques = tuple(sorted_uniques)
                        multiple_target_type_set.add(tuple_uniques)

                        try:
                            trigger_cases[param] += 1
                        except:
                            trigger_cases[param] = 1
        except StopIteration:
            break

    for param in params_to_check:
        if param in trigger_cases:
            print "Multiple unexpected values for {}".format(param)
        else:
            print "Parameter '{}' is homogeneous across requests".format(param)

    print multiple_target_type_set

def check_request_areas(df):
    # This one has to be done separately because the contained dicts are not
    # hashable.
    pass

def check_exposure_homogeneity(df):
    # Check that all rfames within an exposure have the same EXPTIME, OBJECT,
    #   INSTRUME and FILTER
    print "Checking homogeneity of frames in the same exposure..."
    params_to_check = ['EXPTIME','OBJECT','INSTRUME','FILTER']
    exp_grouper = group_generator(df,['REQNUM','DATE_OBS'])
    show_columns = ['id','EXPTIME','OBSTYPE','OBJECT','INSTRUME','FILTER','RLEVEL']

    trig_cases = {}
    frame_nums = {}
    frame_num_examples = {}
    while True:
        try:
            val, group = next(exp_grouper)
            n = len(group)
            try:
                frame_nums[n] += 1
                frame_num_examples[n].append(group)
            except:
                frame_nums[n] = 1
                frame_num_examples[n] = [group]

            if len(group) > 1:
                for param in params_to_check:
                    if len(group[param].unique()) > 1:
                        try:
                            trig_cases[param] += 1
                        except:
                            trig_cases[param] = 1
        except StopIteration:
            break

    if len(trig_cases) > 0:
        print "WARNING: Expected parameters are not homogeneous across exposure frames:"
        print "Cases of differences:"
        for key in trig_cases:
            print "{}: {}".format(key,trig_cases[key])
    else:
        print "Success: exposure frames are indeed homogeneous"

    for num in sorted(frame_nums.keys()):
        print "Exposures with {} frame(s): {}".format(num,frame_nums[num])

    for num in frame_num_examples:
        types = set()
        for g in frame_num_examples[num]:
            types.update(set(g['OBSTYPE'].unique()))
        print "{}: {}".format(num,types)

# def check_arc_lampflat_previous_exposure(df):
#     # For spectrum exposures preceeded by a lampflat and arc, sometimes the
#     # calibration measurements will be while the telescope is pointng at a
#     # different target. Check whether this target is from the previous exposure
#     # (i.e. the telescope hasn't moved). Alternative theory is that it could be
#     # the previous Proposal target? Unlikely.
#     df_sorted = df.sort_values('DATE_OBS')
#     # relevant_cols = ['REQNUM','DATE_OBS','EXPTIME','INSTRUME','FILTER','OBJECT',
#     #     'OBSTYPE','RLEVEL','PROPID']
#     # req_grouper = df.groupby(df,['REQNUM'])
#     # previous = None
#     # while True:
#     #     val, group = next(grouper)
#     #     if len(group) > 1:
#     #         if len(group['OBJECT'].unique()) > 1:
#     #             print
#     data_dict = {}
#     grouped_by_request = df_sorted.groupby('REQNUM')
#     for val, group in grouped_by_request:
#         finish_time = group['DATE_OBS'].iloc[-1]
#         for index, _ in group.iterrows():
#             data_dict[index] = finish_time
#
#     df_sorted['finish_time'] = df.index.to_series().apply(lambda x: data_dict[x])
#     print df_sorted
#     return




    # grouper = group_generator(df,group_on)
    # while True:
    #     val, group = next(grouper)
    #     if len(group) > 1:
    #         print group[check].unique()
    #         if look_for != None:
    #             print group.loc[:,look_for]
    #         x = raw_input("")

def reduce_frames(df):
    obs_groups = df.groupby('datetime')
    print "Expected number of frames:", len(obs_groups)

    for d, obs_frames in obs_groups:
        temp = obs_frames[ obs_frames['RLEVEL'] == obs_frames['RLEVEL'].max() ]
        if len(temp) > 1:
            unique_values = [ tuple(sorted(arr)) for arr in temp[['RLEVEL','OBSTYPE']].values ]
            if len(pd.unique(unique_values)) == 1:
                for c in temp[['datetime','EXPTIME','FILTER','INSTRUME','OBJECT','OBSTYPE','PROPID','REQNUM','RLEVEL','RA','DEC']].columns:
                    if len(temp[c].unique()) > 1:
                        print temp[c]
            else:
                print pd.unique(unique_values)

    new_df = pd.concat([
        obs_frames[ obs_frames['RLEVEL'] == obs_frames['RLEVEL'].max() ]
        for d, obs_frames in obs_groups ]
    )
    print "Actual number of frames:", len(new_df)
    print "Success!"

################################################################################

if __name__ == '__main__':
    df = merge_datasets('data/lco_data/coj_2m0a_2016-02-01_2016-08-01')
    check_reqnums(df)
    check_request_homogeneity(df)
    # check_exposure_homogeneity(df)
    # check_arc_lampflat_previous_exposure(df)
