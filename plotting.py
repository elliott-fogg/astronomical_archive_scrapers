import plotly.offline as py
import plotly.graph_objs as go
from get_dataframe_lco_default import *
import datetime, re
from numpy import log10, finfo
from os.path import join as pathjoin

# TODO: Ensure all graphs have correct units, axis labels and titles
#       Check fits data for 0-second spectrum frames


## Patterns by Proposal
def print_proposal_patterns(block_list):
    for propid, group in block_list.groupby('propid'):
        print propid
        pattern_list = []
        for pattern, g2 in group.groupby('pattern'):
            pattern_list.append( [len(g2),pattern] )
        for p in sorted(pattern_list,key=lambda x: x[0],reverse=True):
            print p[0], p[1]
        print ""


## Time Distribution
def plot_obstype_time_distribution(df):
    def roundtime15(dto):
        return dto - datetime.timedelta(minutes=dto.minute%15,
            seconds=dto.second, microseconds=dto.microsecond)
    def bintime(dto):
        rounded = roundtime15(dto)
        timebin = rounded.hour * 4 + rounded.minute/15
        return timebin

    df['timebin'] = df['datetime'].apply(bintime)

    timebin_data = {}
    for obstype, type_group in df.groupby('OBSTYPE'):
        x_data = [i / 4. for i in range(24*4)]
        y_data = [0 for _ in range(24*4)]
        for timebin, timebin_group in type_group.groupby('timebin'):
            y_data[timebin] = len(timebin_group)
        type_timebins = [(x_data[i],y_data[i]) for i in range(24*4) if y_data[i] != 0]
        timebin_data[obstype] = type_timebins

    bar_data = [
        go.Bar(
            x=[e[0] for e in data],
            y=[e[1] for e in data],
            name=obstype
        ) for obstype, data in timebin_data.items()
    ]
    layout = go.Layout(
        barmode='stack',
        title='Time-of-Day (UT) distribution of different Observation Types',
        xaxis={
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 0.25,
            'title': 'Time of day (UT)'
        },
        yaxis={
            'title': 'Number of observations'
        }
    )
    fig = go.Figure(data=bar_data, layout=layout)
    py.plot(fig,filename='plots/lco_coj_obstype_time_distribution.html')


## Orphaned Calibration Frames
def get_orphan_data(bl):
    orphan_data = {}
    for proposal_id, prop_group in bl.groupby('propid'):
        orphan_true = prop_group[ prop_group['orphan'] == True ]
        orphan_false = prop_group[ prop_group['orphan'] == False ]
        prop_data = {
            'true_count': len(orphan_true),
            'true_time': orphan_true.duration.sum()/3600.,
            'false_count': len(orphan_false),
            'false_time': orphan_false.duration.sum()/3600.,
        }
        prop_data['percentage'] = \
            prop_data['true_time'] / float(prop_data['true_time'] + prop_data['false_time'])
        orphan_data[proposal_id] = prop_data
    return orphan_data

def plot_orphan_science_count(orphan_data):
    bar_data = [
        go.Bar(
            x=['Orphaned','Not Orphaned'],
            y=[data['true_count'],data['false_count']],
            name=propid
        ) for propid, data in orphan_data.items()
    ]
    layout = go.Layout(
        barmode='stack',
        title="Orphaned Science Blocks (Count)",
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_orphan_count.html')

def plot_orphan_science_time(orphan_data):
    bar_data = [
        go.Bar(
            x=['Orphaned','Not Orphaned'],
            y=[ data['true_time'], data['false_time']],
            name=propid
        ) for propid, data in orphan_data.items()
    ]
    layout = go.Layout(
        barmode='stack',
        title="Orphaned Science Blocks (Exposure Time)"
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_orphan_time.html')

def plot_orphan_percentages(orphan_data):
    perc_data = [ (p, d['percentage']) for p,d in orphan_data.items()]
    perc_data.sort(key=lambda x: x[1],reverse=True)
    bar_data = [
        go.Bar(
            x=[e[0] for e in perc_data],
            y=[e[1] * 100 for e in perc_data]
        )
    ]
    layout = go.Layout(
        title="Orphaned Science Time per Proposal for 2016A(ish)",
        yaxis={
            'title': 'Portion of Debited Time that was orphaned (%)'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_orphan_percentages.html')

def plot_orphan_proportions(orphan_data):
    data = sorted([ (p, d['true_time'], d['false_time'],d['percentage']) for \
        p, d in orphan_data.items() ],key=lambda x: x[1],reverse=True)

    text_template = "<em>{}</em> hrs not Orphaned<br>" + \
        "<em>{}</em> hours Orphaned (<em>{}%</em>)"

    bar_data = [
        go.Bar(
            x=[ e[0] for e in data ],
            y=[ e[1] for e in data ],
            name='Orphaned',
            hoverinfo='none',
            marker={
                'color': 'rgb(255,0,0)'
            }
        ),
        go.Bar(
            x=[ e[0] for e in data ],
            y=[ e[2] for e in data ],
            name='Not Orphaned',
            text=[ text_template.format(\
                round(e[2],2),round(e[1],2),round(e[3]*100,1)) for e in data],
            hoverinfo='text',
            marker={
                'color': 'rgb(202,204,206)'
            }
        )
    ]
    layout = go.Layout(
        barmode='stack',
        title='Orphaned (Scheduled Block with no Science Frame) vs Non-Orphaned time',
        yaxis={
            'title': 'Debited Time (Hours)'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_orphan_portions.html')


### Distribution of science exposure times per proposal (layered histogram)
def plot_science_exposure_times_default(bl):
    sci_exp_data = {}
    for proposal_id, prop_group in bl.groupby('propid'):
        prop_data = {}
        for tup in prop_group.science_exposure_times:
            for t in tup:
                if t in prop_data:
                    prop_data[t] += 1
                else:
                    prop_data[t] = 1
        sci_exp_data[proposal_id] = prop_data

    bar_data = [
        go.Bar(
            x=[int(float(p)) for p in data],
            y=[data[p] for p in data],
            name=propid
        ) for propid, data in sci_exp_data.items()
    ]

    layout = go.Layout(
        barmode='stack',
        title='Exposure Time Distribution',
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_exposure_distribution.html')

def plot_science_exposure_times_categories(bl):
    exp_set = set()
    sci_exp_data = {}
    for proposal_id, prop_group in bl.groupby('propid'):
        prop_data = {}
        for tup in prop_group.science_exposure_times:
            for t in tup:
                exp_set.add(t)
                if t in sci_exp_data:
                    prop_data[t] += 1
                else:
                    prop_data[t] = 1
        sci_exp_data[proposal_id] = prop_data

    x_keys = sorted( [ (x,int(float(x))) for x in exp_set ],key=lambda x: x[1] )

    x_values = [e[1] for e in x_keys]
    bar_data = [
        go.Bar(
            x=x_values,
            y=[0 for _ in x_values],
            name="",
            hoverinfo='none',
            showlegend=False
        )
    ]

    bar_data += [
        go.Bar(
            x=[int(float(p)) for p in data],
            y=[data[p] for p in data],
            name=propid
        ) for propid, data in sci_exp_data.items()
    ]

    layout = go.Layout(
        barmode='stack',
        title='Exposure Time Distribution',
        xaxis={
            'type': 'category'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_exposure_distribution.html')

def plot_science_exposure_times_log(bl):
    sci_exp_data = {}
    bl = bl.loc[ bl['science_exposure_times'] > 0 ]
    for proposal_id, prop_group in bl.groupby('propid'):
        prop_data = {}
        for tup in prop_group.science_exposure_times:
            for t in tup:
                if t in prop_data:
                    prop_data[t] += 1
                else:
                    prop_data[t] = 1
        sci_exp_data[proposal_id] = prop_data

    bar_data = [
        go.Bar(
            x=[log10(float(p)) for p in data],
            y=[data[p] for p in data],
            name=propid,
            text=["Length: <em>{}</em>s<br>Count: <em>{}</em>".format(\
                int(float(p)), data[p]) for p in data],
            hoverinfo='text+name'
        ) for propid, data in sci_exp_data.items()
    ]

    labels = [1,5,10,50,100,500,1000]
    tickvals = [0,0.7,1,1.7,2,2.7,3]

    layout = go.Layout(
        barmode='stack',
        title='Exposure Time Distribution',
        xaxis=go.layout.XAxis(
            ticktext=labels,
            tickvals=tickvals,
            title='Exposure Time (s)'
        ),
        yaxis={
            'title': 'Count'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_exposure_distribution_log.html')

def plot_science_exposure_sums(bl):
    sci_exp_data = {}
    for proposal_id, prop_group in bl.groupby('propid'):
        prop_data = {}
        for tup in prop_group.science_exposure_times:
            total = 0
            for t in tup:
                total += float(t)
            total = round(total)
            if total == 0:
                continue
            if total in prop_data:
                prop_data[total] += 1
            else:
                prop_data[total] = 1
        sci_exp_data[proposal_id] = prop_data

    bar_data = [
        go.Bar(
            x=[log10(float(p)) for p in data],
            y=[data[p] for p in data],
            name=propid,
            text=["{}s<br>{}".format(\
                round(float(p),1), data[p]) for p in data],
            hoverinfo='text+name'
        ) for propid, data in sci_exp_data.items()
    ]

    labels = [1,5,10,50,100,500,1000]
    tickvals = [0,0.7,1,1.7,2,2.7,3]

    layout = go.Layout(
        barmode='stack',
        title='Exposure Sum (Sum of all science exposures in a single block) Distribution',
        xaxis=go.layout.XAxis(
            ticktext=labels,
            tickvals=tickvals,
            title='Sum Exposure Length (s)'
        ),
        yaxis={
            'title': 'Count'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_exposure_sum_distribution_log.html')


### mean_ra distribution per proposal
def plot_mean_ra_by_count(bl):
    # This also includes moving targets. Should it not?
    ra_data = {}
    bl['ra_bin'] = bl.mean_ra.apply(lambda x: int(x*4.))
    for propid, prop_group in bl.groupby('propid'):
        x_data = [i/4. for i in range(24*4)]
        y_data = [ 0 for _ in range(24*4) ]
        for bin_num in prop_group['ra_bin']:
            y_data[bin_num] += 1
        prop_data = [(x_data[i],y_data[i]) for i in range(24*4) if y_data[i] != 0]
        ra_data[propid] = prop_data

    bar_data = [
        go.Bar(
            x=[e[0] for e in data],
            y=[e[1] for e in data],
            name=propid
        ) for propid, data in ra_data.items()
    ]
    layout = go.Layout(
        barmode='stack',
        title='RA Distribution',
        xaxis={
            'title': 'Right Ascension (Hours)'
        },
        yaxis={
            'title': 'Observation Count'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_ra_count.html')

def plot_mean_ra_by_time(bl):
    ra_data = {}
    bl['ra_bin'] = bl.mean_ra.apply(lambda x: int(x*4.))
    for propid, prop_group in bl.groupby('propid'):
        x_data = [ i/4. for i in range(24*4) ]
        y_data = [ 0 for _ in range(24*4) ]
        for row in prop_group[['ra_bin','science_exposure_sum']].values:
            bin_num = int(row[0])
            exp_time = row[1] / 3600.
            y_data[bin_num] += exp_time
        prop_data = [ (x_data[i],round(y_data[i],2)) for i in range(24*4) if y_data[i] != 0]
        ra_data[propid] = prop_data

    bar_data = [
        go.Bar(
            x=[e[0] for e in data],
            y=[round(e[1],2) for e in data],
            name=propid
        ) for propid, data in ra_data.items()
    ]
    layout = go.Layout(
        barmode='stack',
        title='RA Distribution',
        xaxis={
            'title': 'Right Ascension (Hours)'
        },
        yaxis={
            'title': 'Total Science Exposure Time (Hrs)'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_ra_time.html')


### mean_dec distribution per proposal
def plot_mean_dec_by_count(bl):
    # This also includes moving targets. Should it not?
    dec_data = {}
    bl['dec_bin'] = bl.mean_dec.apply(lambda x: int(x/10.)+9)
    for propid, prop_group in bl.groupby('propid'):
        x_data = [(i-9)*10 for i in range(19)]
        y_data = [0 for _ in range(19)]
        for bin_num in prop_group['dec_bin']:
            y_data[bin_num] += 1
        prop_data = [(x_data[i],y_data[i]) for i in range(19) if y_data[i] != 0]
        dec_data[propid] = prop_data

    bar_data = [
        go.Bar(
            x=[(i-9)*10 for i in range(19)],
            y=[0 for _ in range(19)],
            hoverinfo='none',
            showlegend=False
        )
    ]
    bar_data += [
        go.Bar(
            x=[e[0] for e in data],
            y=[e[1] for e in data],
            text=[str(e[1]) for e in data],
            hoverinfo='text+name',
            name=propid
        ) for propid, data in dec_data.items()
    ]
    layout = go.Layout(
        barmode='stack',
        title='Dec. Distribution',
        xaxis={
            'title': 'Declination (Degrees)'
        },
        yaxis={
            'title': 'Observation Count'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_dec_count.html')

def plot_mean_dec_by_time(bl):
    # This also includes moving targets. Should it not?
    dec_data = {}
    bl['dec_bin'] = bl.mean_dec.apply(lambda x: int(x/10.)+9)
    for propid, prop_group in bl.groupby('propid'):
        x_data = [(i-9)*10 for i in range(19)]
        y_data = [0 for _ in range(19)]
        for row in prop_group[['dec_bin','science_exposure_sum']].values:
            bin_num = int(row[0])
            exp_time = row[1] / 3600.
            y_data[bin_num] += exp_time
        prop_data = [(x_data[i],round(y_data[i],2)) for i in range(19) if y_data[i] != 0]
        dec_data[propid] = prop_data

    bar_data = [
        go.Bar(
            x=[(i-9)*10 for i in range(19)],
            y=[0 for _ in range(19)],
            hoverinfo='none',
            showlegend=False
        )
    ]
    bar_data += [
        go.Bar(
            x=[e[0] for e in data],
            y=[e[1] for e in data],
            text=[str(e[1]) for e in data],
            hoverinfo='text+name',
            name=propid
        ) for propid, data in dec_data.items()
    ]
    layout = go.Layout(
        barmode='stack',
        title='Dec. Distribution',
        xaxis={
            'title': 'Declination (Degrees)'
        },
        yaxis={
            'title': 'Total Science Exposure Time (Hrs)'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_dec_time.html')


## Duration distribution per proposal



## Various efficiency graphs
def plot_science_exposure_efficiency(bl):
    eff_data = []
    for propid, prop_group in bl.groupby('propid'):
        tot_exp_time = prop_group.exposure_sum.sum() / 3600.
        sci_exp_time = prop_group.science_exposure_sum.sum() / 3600.

        eff_data.append( (propid, tot_exp_time, sci_exp_time) )


    bar_data = [
        go.Bar(
            x=[ e[0] for e in eff_data ],
            y=[ e[2] for e in eff_data ],
            name='Science',
            marker={
                'color': 'rgb(255,0,0)'
            },
            hoverinfo='none'
        ),
        go.Bar(
            x=[ e[0] for e in eff_data ],
            y=[ e[1]-e[2] for e in eff_data ],
            name='Calibration',
            marker={
                'color': 'rgb(255,180,0)'
            },
            text=["Calibration: {} ({}%)<br>Science: {} ({}%)".format(\
                round(e[1]-e[2],1),round((e[1]-e[2])/e[1]*100.,1),round(e[2],1),
                round(e[2]/e[1]*100.,1)) for e in eff_data],
            hoverinfo='text'
        )
    ]
    layout = go.Layout(
        barmode='stack',
        title='Science vs Total Exposure Time',
        yaxis={
            'title': 'Exposure Time (Hours)'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_science_efficiency.html')

# Percentage of moving targets per proposal
def plot_moving_count(bl):
    move_data = []
    for propid, prop_group in bl.groupby('propid'):
        move = len(prop_group[ prop_group.moving == True ])
        stat = len(prop_group[ prop_group.moving == False ])
        ratio = move / (move + stat + finfo(float).eps) * 100.
        move_data.append( (propid, move, stat, ratio) )

    move_data.sort(key=lambda x: x[3],reverse=True)

    bar_data = [
        go.Bar(
            x=[ e[0] for e in move_data ],
            y=[ e[2] for e in move_data ],
            name='Stationary',
            hoverinfo='none',
        ),
        go.Bar(
            x=[ e[0] for e in move_data ],
            y=[ e[1] for e in move_data ],
            name='Moving',
            text=[ "Moving: {} ({}%)<br>Stationary: {} ({}%)".format(\
                e[1], round(e[3],1), e[2], round(100.-e[3],1)) for e in move_data],
            hoverinfo='text',
        )
    ]
    layout = go.Layout(
        barmode='stack',
        title='Proportions of Moving Targets (Count)',
        yaxis={
            'title': 'Observations'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_moving_count.html')

def plot_moving_time(bl):
    move_data = []
    for propid, prop_group in bl.groupby('propid'):
        move = prop_group[ prop_group.moving == True \
            ].science_exposure_sum.sum() / 3600.
        stat = prop_group[ prop_group.moving == False \
            ].science_exposure_sum.sum() / 3600.
        ratio = move / (move + stat + finfo(float).eps) * 100.
        move_data.append( (propid, move, stat, ratio) )

    move_data.sort(key=lambda x: x[3],reverse=True)

    bar_data = [
        go.Bar(
            x=[ e[0] for e in move_data ],
            y=[ e[2] for e in move_data ],
            name='Stationary',
            hoverinfo='none',
        ),
        go.Bar(
            x=[ e[0] for e in move_data ],
            y=[ e[1] for e in move_data ],
            name='Moving',
            text=[ "Moving: {} ({}%)<br>Stationary: {} ({}%)".format(\
                round(e[1],2), round(e[3],1), round(e[2],2), round(100.-e[3],1))
                for e in move_data],
            hoverinfo='text',
        )
    ]
    layout = go.Layout(
        barmode='stack',
        title='Proportions of Moving Targets (Exposure Time)',
        yaxis={
            'title': 'Exposure Time (hours)'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_moving_time.html')

# Use of telescope instruments per proposal
def plot_instrument_count(bl):
    inst_data = []
    for propid, prop_group in bl.groupby('propid'):
        i_en05 = len(prop_group[ prop_group.instrument == 'en05' ])
        i_fs01 = len(prop_group[ prop_group.instrument == 'fs01' ])
        ratio = i_en05 / (i_en05 + i_fs01 + finfo(float).eps) * 100.
        inst_data.append( (propid, i_en05, i_fs01, ratio) )

    inst_data.sort(key=lambda x: x[3],reverse=True)

    bar_data = [
        go.Bar(
            x=[ e[0] for e in inst_data ],
            y=[ e[1] for e in inst_data ],
            name='en05',
            hoverinfo='none',
        ),
        go.Bar(
            x=[ e[0] for e in inst_data ],
            y=[ e[2] for e in inst_data ],
            name='fs01',
            text=[ "fs01: {} ({}%)<br>en05: {} ({}%)".format(\
                e[2], round(100.-e[3],1), e[1], round(e[3],1)) for e in inst_data],
            hoverinfo='text',
        )
    ]
    layout = go.Layout(
        barmode='stack',
        title='Proportions of Instrument Usage (Count)',
        yaxis={
            'title': 'Observations per instrument'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_instrument_count.html')

def plot_instrument_time(bl):
    inst_data = []
    for propid, prop_group in bl.groupby('propid'):
        i_en05 = prop_group[ prop_group.instrument == 'en05' \
            ].science_exposure_sum.sum() / 3600.
        i_fs01 = prop_group[ prop_group.instrument == 'fs01' \
            ].science_exposure_sum.sum() / 3600.
        ratio = i_en05 / (i_en05 + i_fs01 + finfo(float).eps) * 100.
        inst_data.append( (propid, i_en05, i_fs01, ratio) )

    inst_data.sort(key=lambda x: x[3],reverse=True)

    bar_data = [
        go.Bar(
            x=[ e[0] for e in inst_data ],
            y=[ e[1] for e in inst_data ],
            name='en05',
            hoverinfo='none',
        ),
        go.Bar(
            x=[ e[0] for e in inst_data ],
            y=[ e[2] for e in inst_data ],
            name='fs01',
            text=[ "fs01: {} ({}%)<br>en05: {} ({}%)".format(\
                round(e[2],2), round(100.-e[3],1), round(e[1],2), round(e[3],1))
                for e in inst_data],
            hoverinfo='text',
        )
    ]
    layout = go.Layout(
        barmode='stack',
        title='Proportions of Instrument Usage (Exposure Time)',
        yaxis={
            'title': 'Exposure Time per instrument (hours)'
        }
    )
    fig = go.Figure(data=bar_data,layout=layout)
    py.plot(fig,filename='plots/lco_coj_instrument_time.html')


# Distribution of observation-starts over the semester (e.g. chunk into 5-day
# bins) for each proposal.

def plot_graphs(dataset_fullname):
    #
    if not os.path.isdir("plots"):
        os.mkdir("plots")
    #
    df, bl = setup(dataset_fullname)
    #
    print "Plotting graphs for 'dataset_fullname'"
    #
    plot_obstype_time_distribution(df)
    #
    plot_orphan_proportions(get_orphan_data(bl))
    #
    plot_science_exposure_times_log(bl)
    plot_science_exposure_sums(bl)
    #
    plot_mean_ra_by_count(bl)
    plot_mean_ra_by_time(bl)
    plot_mean_dec_by_count(bl)
    plot_mean_dec_by_time(bl)
    #
    plot_science_exposure_efficiency(bl)
    #
    plot_instrument_count(bl)
    plot_instrument_time(bl)
    #
    plot_moving_count(bl)
    plot_moving_time(bl)


def clear_all_graphs():
    yesno = raw_input("Are you sure you want to delete all existing graphs? [y,N] ")
    if yesno in ('y','Y'):
        pass


def plot_all():
    # Select dataset(s) to run for

    # Get list of all datasets
    # Number, display datasets
    # Accept input of datasets
    # Confirm?
    # Execute for each dataset
    available_datasets = []
    dataset_type_list = os.listdir("data")
    for dataset_type in dataset_type_list:
        for dataset_name in os.listdir(pathjoin("data",dataset_type)):
            dataset_fullname = pathjoin(dataset_type,dataset_name)
            if os.path.isfile(pathjoin("data",dataset_fullname,"_complete")):
                available_datasets.append(dataset_fullname)

    if len(available_datasets) == 0:
        print "No completed datasets available."
        return

    counter = 0
    print "\nAvailable Datasets:"
    for dataset_fullname in available_datasets:
        print "{}: {}".format(counter, re.sub("[_/]", " ", dataset_fullname))
        counter += 1

    no_errors = False
    while no_errors == False:
        no_errors = True

        selection = raw_input("\nSelect datasets to plot, or 'N' to abort: ").split()
        valid_selections = []

        for i in selection:
            if i in ('n','N'):
                print "Plotting aborted by request."
                return
            try:
                i = int(i)
            except ValueError:
                no_errors = False
                print "'{}' is not valid.".format(i)
                print "Input only space-separated integers."
                continue
            if i >= len(available_datasets) or i < 0:
                no_errors = False
                print "'{}' is beyond allowed values.".format(i)
                continue
            valid_selections.append(i)

    print "\nThe following datasets were selected:"
    for i in valid_selections:
        print "* " + re.sub("[_/]", " ", available_datasets[i])
    yesno = raw_input("\nIs this correct? [y/N] ")
    if yesno in ('y','Y'):
        for i in valid_selections:
            plot_graphs(available_datasets[i])
    else:
        print "Plotting aborted."



## Try and extract the mean cadence for repeated observations (repeated meaning
#   same pattern, propid, target). Get the average gap for each proposal, or
#   each type of pattern (with each proposal having patterns distinct from other
#   proposals)

################################################################################

if __name__ == '__main__':
    yesno = raw_input('Run IPython setup? [y/N] ')
    if yesno in ('y','Y'):
        df, bl, raw_df = setup(True)
    else:
        plot_all()
