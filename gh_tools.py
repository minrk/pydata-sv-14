from datetime import datetime

import pandas as pd

from gh_api import get_pulls_list


ISO8601 = "%Y-%m-%dT%H:%M:%SZ"

def _parse_datetime(s):
    """Parse dates in the format returned by the Github API."""
    if s:
        return datetime.strptime(s, ISO8601)
    else:
        return None

def pulls_to_data_frame(pulls):
    """convert list of PR dicts to a data frame"""
    # filter out those with invalid user or still open
    filtered = [ p for p in pulls if p['user'] and p['closed_at']]

    df = pd.DataFrame(
        [(pr['number'], pr['user']['login'], pr['milestone']['title'] if pr['milestone'] else None,
              _parse_datetime(pr['created_at']),
            pr['merged_at'] is not None,
            _parse_datetime(pr['closed_at']),
            ) for pr in filtered ],
        columns=['number', 'user', 'milestone', 'opened', 'merged', 'closed'],
        )
    return df
    
    df.head()


core_devs = {
    'fperez',
    'ellisonbg',
    'takluyver',
    'ivanov',
    'jdfreder',
    'minrk',
    'Carreau',
}

def core_non_core(df):
    """count PRs from core and non-core devs in a given data frame"""
    core_mask = df['user'].map(lambda u: u in core_devs)
    core_count = core_mask.sum()
    non_core_count = len(df) - core_count
    return core_count, non_core_count


def monthly_data(df, year_start, month_start=1, year_end=None, month_end=None):
    """monthly summary of PR activity"""
    data = []
    
    today = datetime.now()
    if month_end is None:
        month_end = today.month
    if year_end is None:
        year_end = today.year
    
    for year in range(year_start, year_end + 1):
        for month in range(1, 13):
            if year == year_start and month < month_start:
                continue
            elif year == year_end and month > month_end:
                continue
            start = datetime(year=year, month=month, day=1)
            if month == 12:
                end = datetime(year=year+1, month=1, day=1)
            else:
                end = datetime(year=year, month=month+1, day=1)
        
            mask = (df['closed'] < end) * (df['closed'] > start)
            month_prs = df[mask]
            merged = month_prs[month_prs['merged']]
            core_merged, nc_merged = core_non_core(merged)
        
            rejected = month_prs[month_prs['merged'] == False]
            core_rejected, nc_rejected = core_non_core(rejected)
            data.append(
                [start.date(), core_merged, nc_merged, core_rejected, nc_rejected]
            )
    df = pd.DataFrame(data, columns=['date', 'cm', 'ncm', 'cr', 'ncr'])
    df.index = df['date']
    del df['date']
    
    merged = df[['cm', 'ncm']]
    merged.columns = ['core', 'community']
    rejected = df[['cr', 'ncr']]
    rejected.columns = ['core', 'community']
    
    return merged, rejected
