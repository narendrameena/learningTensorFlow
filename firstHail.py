#!/usr/bin/env python

#author narumeena 
#description Learning hail from hail tutorial for later implementation on tensorflow with hail 

from hail import *
##create a hail object 
hc = HailContext()


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from collections import Counter
from math import log, isnan
from pprint import pprint
#%matplotlib inline
#from IPython import get_ipython
#get_ipython().run_line_magic('matplotlib', 'osx')
import seaborn


##downlaoding a data file 
import os
if os.path.isdir('data/1kg.vds') and os.path.isfile('data/1kg_annotations.txt'):
    print('All files are present and accounted for!')
else:
    import sys
    sys.stderr.write('Downloading data (~50M) from Google Storage...\n')
    import urllib
    import tarfile
    urllib.urlretrieve('https://storage.googleapis.com/hail-1kg/tutorial_data.tar',
                       'tutorial_data.tar')
    sys.stderr.write('Download finished!\n')
    sys.stderr.write('Extracting...\n')
    tarfile.open('tutorial_data.tar').extractall()
    if not (os.path.isdir('data/1kg.vds') and os.path.isfile('data/1kg_annotations.txt')):
        raise RuntimeError('Something went wrong!')
    else:
        sys.stderr.write('Done!\n')


#loading data from th disk 
vds = hc.read('data/1kg.vds')

#summrize your data : its alwayes fun to get know your data 
vds.summarize().report()

#pull out 5 variant from the data 
vds.query_variants('variants.take(5)')

#other ways 

vds.query_samples('samples.take(5)')

vds.sample_ids[:5]

#looking at first genotype calls 
vds.query_genotypes('gs.take(5)')


##anotation data 

table = hc.import_table('data/1kg_annotations.txt', impute=True).key_by('Sample')
##print schema 
pprint(table.schema)