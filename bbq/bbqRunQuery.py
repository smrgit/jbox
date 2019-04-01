import logging
import pandas as pd
import time

def bbqRunQuery ( client, qString, dryRun=False ):
  
  logging.debug ( "\n in bbqRunQuery ... " )
  if ( dryRun ):
    logging.debug ( "    dry-run only " )
    
  ## set up QueryJobConfig object
  job_config = bigquery.QueryJobConfig()
  job_config.dry_run = dryRun
  job_config.use_query_cache = True
  job_config.use_legacy_sql = False
  
  ## run the query
  try:
    logging.debug ( "    submitting query ... " )
    query_job = client.query ( qString, job_config=job_config )
    logging.debug ( "    query job state: ", query_job.state )
  except:
    print ( "  FATAL ERROR: query submission failed " )
    return ( None )
  
  if ( not dryRun ):
    query_job = client.get_job ( query_job.job_id )
    logging.debug ( ' Job {} is currently in state {}'.format ( query_job.job_id, query_job.state ) )
    while ( query_job.state != "DONE" ):
      time.sleep ( 1 )
      query_job = client.get_job ( query_job.job_id )
      logging.debug ( ' Job {} is currently in state {}'.format ( query_job.job_id, query_job.state ) )
  
  ## return results as a dataframe (or an empty dataframe for a dry-run) 
  if ( not dryRun ):
    df = query_job.to_dataframe()
    if ( query_job.total_bytes_processed==0 ):       
      logging.debug ( "    the results for this query were previously cached " )
    else:
      logging.debug ( "    this query processed {} bytes ".format(query_job.total_bytes_processed) )
    
    ## if ( len(df) < 1 ): logging.warning ( "  WARNING: this query returned NO results ")
    
    logging.debug ( " --> returning dataframe ... ", len(df) )
    return ( df )
    
  else:
    logging.info ( "    if not cached, this query will process {} bytes ".format(query_job.total_bytes_processed) )
    ## return an empty dataframe
    return ( pd.DataFrame() )

