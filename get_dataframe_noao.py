import pandas as pd
import datetime as dt
from os.path import join as pathjoin
import astropy

def open_votable(filepath):
    table = astropy.io.votable.parse_single_table(filepath)
    return table.to_table(use_names_over_ids=True).to_pandas()

################################################################################

if __name__ == '__main__':
    ndf10 = open_votable("./data/noao/test_table_10.vot")
    ndff = open_votable("./data/noao/test_table_full.vot")
