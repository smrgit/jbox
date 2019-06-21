#!/usr/bin/env python

from google import auth
from google.cloud import bigquery

import bbq
import sys
import time

##------------------------------------------------------------------------------

## !!! NB -- the strings below need to be replaced by VALID GCP 
## !!!       project ids and a pre-existing BQ dataset name 
YOUR_BILLING_PROJECT = "insert-valid-GCP-project-to-run-BQ-job"
YOUR_OUTPUT_PROJECT = "insert-valid-GCP-project-to-write-BQ-table"
YOUR_OUTPUT_DATASET = "insert-valid-BQ-dataset-name"

##------------------------------------------------------------------------------

def getReady ( billingProject ):

    # first we need to do the AUTH step:
    try:
        ( credentials, project ) = auth.default()
    except:
        print ( 'Failed to authenticate ???' )
    
    # then we set up the BigQuery Client():
    try:
        bqclient = bigquery.Client(credentials=credentials, project=billingProject)
    except:
        print ( 'Failed to initialize BigQuery client ??? ')

    return ( bqclient )

##------------------------------------------------------------------------------
## write_disposition options: 'WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'

def lookAtTable ( bqclient, 
                  sourceDataProject, sourceBQdataset, sourceBQtable, 
                  excludedNames, excludedTypes,
                  outputDataProject='', outputBQdataset='',
                  write_disposition='' ):

    ## we do two types of 'explorations': first we look at (any) repeated fields
    ## and then we explore the contents of each field ...

    print ( '\n\n\n' )
    print ( ' ********************************************************************* ' )
    print ( ' in lookAtTable ... ', sourceDataProject, sourceBQdataset, sourceBQtable )

    xr = bbq.bbqExploreRepeatedFields ( bqclient, 
                                        sourceDataProject, sourceBQdataset, sourceBQtable,
                                        verbose=False )

    ## print ( xr )
    ## print ( len(xr) )

    xf = bbq.bbqExploreFieldContents ( bqclient, 
                                       sourceDataProject, sourceBQdataset, sourceBQtable, 
                                       excludedNames, excludedTypes,
                                       verbose=False )
    ## print ( xf )
    ## print ( len(xf) )

    ## next, we write this information out to a pair of output tables
    ## IF we have an outputDataProject specified ...

    if ( outputDataProject != '' ):

        ## get a pointer to the output BQ dataset ...
        dataset_ref = bqclient.dataset ( outputBQdataset, project=outputDataProject )

        ## if we have some field-content information, write that to a table...
        if ( len(xf) > 0 ):
            outputTableName = sourceBQtable + '_fcInfo'
            print ( '         --> writing field-content information to {}.{}.{} '.format(outputDataProject, outputBQdataset, outputTableName) )
            table_ref = dataset_ref.table ( outputTableName )
            xf = xf.astype('str')
            job_config = bigquery.job.LoadJobConfig()
            if ( write_disposition != '' ): job_config.write_disposition = write_disposition
            load_job = bqclient.load_table_from_dataframe ( xf, table_ref, job_config=job_config )

        ## if we have some repeated-fields information, write that to a table...
        if ( len(xr) > 0 ):
            outputTableName = sourceBQtable + '_rfInfo'
            print ( '         --> writing repeated-fields information to {}.{}.{} '.format(outputDataProject, outputBQdataset, outputTableName) )
            table_ref = dataset_ref.table ( outputTableName )
            xr = xr.astype('str')
            job_config = bigquery.job.LoadJobConfig()
            if ( write_disposition != '' ): job_config.write_disposition = write_disposition
            load_job = bqclient.load_table_from_dataframe ( xr, table_ref, job_config=job_config )

    else:

        print ( ' not writing output tables ' )

    return

##------------------------------------------------------------------------------

def main ( ):

    billingProject = YOUR_BILLING_PROJECT
    bqclient = getReady ( billingProject )

    ##--------------------------------------------------------------------------
    ## we'll start with a simple table:

    ## define the precise location of the table of interest ...
    sourceDataProject = "bigquery-public-data"
    sourceBQdataset   = "github_repos"
    sourceBQtable     = "sample_files"

    ## we can exclude specific field names or data types from these
    ## exhaustive 'exploratory' queries to save time/money
    excludedNames = [ "path", "id", "symlink_target" ]
    excludedTypes = [ ]

    ## and if we specify an 'output' BQ dataset, we can write
    ## information about this 'source' table to the 'output' location
    outputDataProject = YOUR_OUTPUT_PROJECT
    outputBQdataset = YOUR_OUTPUT_DATASET

    lookAtTable ( bqclient, 
                  sourceDataProject, sourceBQdataset, sourceBQtable, 
                  excludedNames, excludedTypes,
                  outputDataProject, outputBQdataset, 'WRITE_TRUNCATE' )

    ##--------------------------------------------------------------------------
    ## next a more complex table:

    sourceDataProject = "bigquery-public-data"
    sourceBQdataset = "human_genome_variants"
    sourceBQtable   = "platinum_genomes_deepvariant_variants_20180823"

    excludedNames = [ ]
    excludedTypes = [ "INTEGER", "FLOAT" ]

    lookAtTable ( bqclient, 
                  sourceDataProject, sourceBQdataset, sourceBQtable, 
                  excludedNames, excludedTypes,
                  outputDataProject, outputBQdataset, 'WRITE_TRUNCATE' )

    return

##------------------------------------------------------------------------------

if __name__ == '__main__':

    t0 = time.time()
    main ( )
    t1 = time.time()

    print ( ' --> time taken in seconds: {dt}'.format(dt=(t1-t0)) )

##------------------------------------------------------------------------------
