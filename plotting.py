import plotly.offline as py
import plotly.graph_objs as go
from get_dataframe_lco_default import *
import datetime

## Patterns by Proposal
def print_proposal_patterns(block_list):
    for propid, group in block_list.groupby('PROPID'):
        print propid
        pattern_list = []
        for pattern, g2 in group.groupby('pattern'):
            pattern_list.append( [len(g2),pattern] )
        for p in sorted(pattern_list,key=lambda x: x[0],reverse=True):
            print p[0], p[1]
        print ""

## Time Distribution
def plot_time_distribution(df):
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
        type_timebins = [0 for _ in range(24*4)]
        for timebin, timebin_group in type_group.groupby('timebin'):
            type_timebins[timebin] = len(timebin_group)
        timebin_data[obstype] = type_timebins

    x_values = [i / 4. for i in range(24*4)]
    bar_data = [
        go.Bar(
            x=x_values,
            y=timebin_data[obstype],
            name=obstype
        ) for obstype in timebin_data
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
            prop_data['true_time'] / (prop_data['true_time'] + prop_data['false_time'])
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

### Distribution of science exposure times per proposal (layered histogram)
# Will need to add in science_exposure_lengths as a tuple in a new column

### mean_ra distribution per proposal

### mean_dec distribution per proposal

## Duration distribution per proposal

## Various efficiency graphs

# Percentage of moving targets per proposal

# Use of telescope instruments per proposal

# Distribution of observation-starts over the semester (e.g. chunk into 5-day
# bins) for each proposal.



## Try and extract the mean cadence for repeated observations (repeated meaning
#   same pattern, propid, target). Get the average gap for each proposal, or
#   each type of pattern (with each proposal having patterns distinct from other
#   proposals)

## Try and see if blocks with multiple targets are ALL just spectrums with the
# first two calibrations being the wrong target

################################################################################

if __name__ == '__main__':
    yesno = raw_input('Run setup? [Y/n] ')
    if yesno in ('y','Y',''):
        df, bl = setup()
