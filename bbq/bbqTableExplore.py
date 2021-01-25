
from google import auth
from google.cloud import bigquery

import argparse
import bbq
import logging
import sys
import time

##------------------------------------------------------------------------------
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

def main ( args ):

    ##--------------------------------------------------------------------------
    ## set up logging
    logger = logging.getLogger('bbq_application')
    logger.setLevel(logging.DEBUG)

    ##--------------------------------------------------------------------------
    ## set up billing project ...

    billingProject = args.billingProject
    bqclient = getReady ( billingProject )

    ##--------------------------------------------------------------------------
    ## we'll start with a simple table:

    ## define the precise location of the table of interest ...
    sourceDataProject = args.sourceDataProject
    sourceBQdataset   = args.sourceBQdataset
    sourceBQtable     = args.sourceBQtable

    ## we can exclude specific field names or data types from these
    ## exhaustive 'exploratory' queries to save time/money
    ## 
    ## for example:  excludedNames = [ "path", "id", "symlink_target" ]
    ##               excludedTypes = [ "INTEGER", "FLOAT" ]
    excludedNames = args.excludedNames
    excludedTypes = args.excludedTypes

    ## and if we specify an 'output' BQ dataset, we can write
    ## information about this 'source' table to the 'output' location
    outputDataProject = args.outputDataProject
    outputBQdataset = args.outputBQdataset

    writeDisposition = args.writeDisposition

    lookAtTable ( bqclient, 
                  sourceDataProject, sourceBQdataset, sourceBQtable, 
                  excludedNames, excludedTypes,
                  outputDataProject, outputBQdataset, writeDisposition )

    return

##------------------------------------------------------------------------------

if __name__ == '__main__':

    t0 = time.time()

    ## we need the following pieces of information:
    ##     sourceDataProject -- required
    ##     sourceBQdataset   -- required
    ##     sourceBQtable     -- required
    ##     outputDataProject -- if not specified, defaults to same as sourceDataProject
    ##     outputBQdataset   -- if not specified, defaults to same as sourceBQdataset
    ##     writeDisposition  -- default: WRITE_TRUNCATE
    ##     billingProject    -- if not specified, defaults to same as sourceDataProject

    parser = argparse.ArgumentParser()

    ## the first three arguments are required -- to fully specify the BQ table of interest
    parser.add_argument ( '-p',  '--sourceProject', action='store', help='source GCP project ID containing BQ dataset and table of interest', required=True, dest='sourceDataProject', type=str )
    parser.add_argument ( '-d',  '--sourceDataset', action='store', help='source Big Query dataset name', required=True, dest='sourceBQdataset', type=str )
    parser.add_argument ( '-t',  '--sourceTable',   action='store', help='source Big Query table name', required=True, dest='sourceBQtable', type=str )

    ## the next two arguments are optional and only needed if the output table(s) need
    ## to go to a different project/dataset than where the input table resides
    parser.add_argument ( '-op', '--outputProject', action='store', help='output GCP project ID -- you must have permission to create a BQ dataset and write a table (will default to same as sourceProject)', required=False, dest='outputDataProject', type=str )
    parser.add_argument ( '-od', '--outputDataset', action='store', help='output Big Query dataset name -- if it does not exist, it will be created (will default to same as sourceDataset)', required=False, dest='outputBQdataset', type=str )

    ## this argument is also optional and defaults to WRITE_TRUNCATE
    parser.add_argument ( '-w',  '--writeDisposition', action='store', help='what to do if the output table already exists (options are WRITE_APPEND, WRITE_EMPTY, and WRITE_TRUNCATE -- defaults to WRITE_TRUNCATE)', required=False, dest='writeDisposition', type=str, default='WRITE_TRUNCATE' )

    ## this argument is only needed if the billing-project is different from the
    ## input or output project(s) ... if not specified, this will initially default
    ## to the output-project and if that is also not specified, it will default to
    ## the input-project
    parser.add_argument ( '-bp', '--billingProject', action='store', help='billing GCP project ID -- you must have permission to run a BQ query and incur charges in this project (will default to same as outputProject)', required=False, dest='billingProject', type=str )

    ## finally, we can optionally EXCLUDE certain fields by name or by type
    parser.add_argument ( '-xn', '--excludedNames', action='store', help='comma-delimited (no spaces, or enclose entire list in quotes) list of field names to be excluded from the exploratory queries', required=False, dest='xNames', type=str )
    parser.add_argument ( '-xt', '--excludedTypes', action='store', help='comma-delimited (no spaces, or enclose entire list in quotes) list of field types to be excluded from the exploratory queries (eg INTEGER and/or FLOAT)', required=False, dest='xTypes', type=str )

    args = parser.parse_args()

    if ( args.outputDataProject is None ): args.outputDataProject = args.sourceDataProject
    if ( args.outputBQdataset is None ): args.outputBQdataset = args.sourceBQdataset

    if ( args.billingProject is None ): args.billingProject = args.outputDataProject

    ## parse out the strings from the xNames and xTypes comma-delimited arguments
    args.excludedNames = [ ]
    args.excludedTypes = [ ]
    if ( args.xNames is not None ): args.excludedNames = [ n.strip() for n in args.xNames.split(',') ]
    if ( args.xTypes is not None ): args.excludedTypes = [ n.strip() for n in args.xTypes.split(',') ]

##     print ( excludedNames )
##     print ( excludedTypes )
## 
##     print ( args )
##     sys.exit(-1)

    main ( args )

    t1 = time.time()

    print ( ' --> time taken in seconds: {dt}'.format(dt=(t1-t0)) )

##------------------------------------------------------------------------------
