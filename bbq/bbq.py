import logging
import pandas as pd
import time

from google.cloud import bigquery

##--------------------------------------------------------------------------------------------------
## this function builds a query to get some basic information about a single
## field which is NOT of type RECORD, in the specified table

## NB: the field of interest can be up to 3 layers 'deep', with a parent,
## and a 'grandparent', in which case the names are all contained in the
## input fNameList, with the child at the end of the list

def bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, fNameList, fModeList ):
  
  logging.debug ( projectName, datasetName, tableName )
  logging.debug ( fNameList, len(fNameList) )
  
  fdepth = len(fNameList)
  
  print ( ' in bbqBuildFieldContentsQuery ... ', fNameList, fModeList, fdepth)
  
  if ( fdepth == 1 ):
    fName = fNameList[0]
    fMode = fModeList[0]
    
    if ( fMode == "REPEATED" ):
      ## construct a query for a REPEATED field
      qString = """
        WITH t1 AS ( SELECT f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{fName} AS f )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)      
    else:
      ## construct a query for a non-REPEATED field
      qString = """
        WITH t1 AS ( SELECT {fName} AS f FROM `{projectName}.{datasetName}.{tableName}` AS t )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)
  
  elif ( fdepth == 2 ):
    pName = fNameList[0]
    fName = fNameList[1]
    
    pMode = fModeList[0]
    fMode = fModeList[1]
    
    if ( fMode == "REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{pName} AS u, u.{fName} AS f )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)
    elif ( pMode == "REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT u.{fName} AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{pName} AS u )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)       
    else:
      qString = """
        WITH t1 AS ( SELECT {pName}.{fName} AS f FROM `{projectName}.{datasetName}.{tableName}` AS t )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)      
     

  elif ( fdepth == 3 ):
    gp = fNameList[0]
    p  = fNameList[1]
    f  = fNameList[2]
    
    gpMode = fModeList[0]
    pMode  = fModeList[1]
    fMode  = fModeList[2]
    
    if ( fMode == "REPEATED" ):
      print ( gp, p, f, fMode )
      print ( " TODO: define query !!! ")
    else:
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT v.{fName} AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName} AS v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 
      
  else:
    qString = "TODO"
  
  return ( qString )


##--------------------------------------------------------------------------------------------------
## this function builds a query to get some basic information about a single
## field which is of type RECORD

def bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, fNameList, fModeList ):
  
  logging.debug ( projectName, datasetName, tableName )
  logging.debug ( fNameList, len(fNameList) )
  
  fdepth = len(fNameList)
  
  if ( fdepth == 1 ):
    fName = fNameList[0]
    fMode = fModeList[0]
    
    if ( fMode == "REPEATED" ):
      ## construct a query for a REPEATED field
      qString = """
        WITH t1 AS ( SELECT ARRAY_LENGTH({fName}) AS f FROM `{projectName}.{datasetName}.{tableName}` AS t )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)      
    else:
      ## construct a query for a non-REPEATED field
      qString = "TODO ???"
  
  elif ( fdepth == 2 ):
    pName = fNameList[0]
    fName = fNameList[1]
    pMode = fModeList[0]
    fMode = fModeList[1]
    if ( fMode == "REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT ARRAY_LENGTH(u.{fName}) AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{pName} AS u )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)      
    else:
      qString = "TODO ???"
      
  else:
    qString = "TODO"
  
  return ( qString )


##--------------------------------------------------------------------------------------------------
## 

def bbqCheckQueryResults ( qr ):
  logging.debug ( "\n in bbqCheckQueryResults ... " )
  if ( not isinstance(qr, pd.DataFrame) ):
    logging.error ( " in bbqCheckQueryResults ... invalid results! " )
    return ( False )
  else:
    if ( len(qr) > 0 ): 
      logging.debug ( " # of rows in query results: {} ".format(len(qr)) )
      logging.debug ( "\n", qr.head() )
    else:
      logging.debug ( " query returned NO results ?!? " )  
    return ( True )


##--------------------------------------------------------------------------------------------------
##

def bbqExploreFieldContents ( bqclient, projectName, datasetName, tableName, excludedNames, excludedTypes ):

  dataset_ref = bqclient.dataset ( datasetName, project=projectName)
  table_ref = dataset_ref.table ( tableName )
  table = bqclient.get_table ( table_ref )
  
  ## outer loop over all fields in schema
  for f in table.schema:  

    ## if this field is a RECORD, then dig down ...
    if ( f.field_type=="RECORD" ):

      print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} [{len(f.fields)}] ' )

      ## loop over all fields within 'f'
      for g in f.fields:     

        ## if this field is also a RECORD, dig further ...
        if ( g.field_type=="RECORD" ):

          print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} [{len(g.fields)}] ' )    

          ## loop over all fields within 'g'
          ## (we are assuming no more RECORDs at or beyond this depth)
          for h in g.fields:

            if ( h.field_type=="RECORD" ):
              logging.error ( " RECORD found at {}>{}>{} ??? ", f.name, g.name, h.name )
              
            else:
            
              if ( h.name not in excludedNames and h.field_type not in excludedTypes ):                
                ## build query to get summary information about field 'h' ...
                qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, 
                                                 [f.name, g.name, h.name], [f.mode, g.mode, h.mode] )
                print ( qs )
                qr = bbqRunQuery ( bqclient, qs )
                if ( not bbqCheckQueryResults ( qr ) ):
                  print ( " Query failed ??? " )
                  print ( qs )
                else:
                  sqr = bbqSummarizeQueryResults ( qr )
                  if ( len(sqr) > 2 ): print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10}', sqr )   
              else:
                print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10} (this field was excluded)' )
                

        ## if g is NOT a RECORD:
        else:
          
          if ( g.name not in excludedNames and g.field_type not in excludedTypes ):
            ## build query to get summary information about field 'g' ...
            qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, 
                                             [f.name, g.name], [f.mode, g.mode] )
            print ( qs )
            qr = bbqRunQuery ( bqclient, qs )
            if ( not bbqCheckQueryResults ( qr ) ):
              print ( " Query failed ??? " )
              print ( qs )
            else:
              sqr = bbqSummarizeQueryResults ( qr )
              if ( len(sqr) > 2 ): print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}', sqr )
          else:
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} (this field was excluded)' )

    ## if f is NOT a RECORD
    else:
      
      if ( f.name not in excludedNames and f.field_type not in excludedTypes ):
        ## build query to get summary information about field 'f' ...
        qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, [f.name], [f.mode] )
        print ( qs )
        qr = bbqRunQuery ( bqclient, qs )
        if ( not bbqCheckQueryResults ( qr ) ):
          print ( " Query failed ??? " )
          print ( qs )
        else:
          sqr = bbqSummarizeQueryResults ( qr )
          if ( len(sqr) > 2 ): print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}', sqr )
      else:
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} (this field was excluded)' )
          
  return ()


##--------------------------------------------------------------------------------------------------
##

def bbqExploreRepeatedFields ( bqclient, projectName, datasetName, tableName ):

  dataset_ref = bqclient.dataset ( datasetName, project=projectName)
  table_ref = dataset_ref.table ( tableName )
  table = bqclient.get_table ( table_ref )
  
  numRF = 0
  
  ## outer loop over all fields in schema
  for f in table.schema:  

    if ( f.mode=="REPEATED" ):
      numRF += 1
      qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                        [f.name], [f.mode] )
      qr = bbqRunQuery ( bqclient, qs )
      sqr = bbqSummarizeQueryResults ( qr )
      if ( sqr[0]==1 ):
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} always repeated {sqr[3]} time(s)' )
      else:  
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}', sqr )
    
    ## if this field is a RECORD, then dig down ...
    if ( f.field_type=="RECORD" ):
      
      if ( f.mode != "REPEATED" ):
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}' )

      ## loop over all fields within 'f'
      for g in f.fields:     

        if ( g.mode=="REPEATED" ):
          numRF += 1
          qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                            [f.name, g.name], [f.mode, g.mode] )
          qr = bbqRunQuery ( bqclient, qs )
          sqr = bbqSummarizeQueryResults ( qr )
          if ( sqr[0] == 1 ):
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} always repeated {sqr[3]} time(s)' )
          else:
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}', sqr )        
        
        ## if this field is also a RECORD, dig further ...
        if ( g.field_type=="RECORD" ):

      
          if ( g.mode != "REPEATED" ):
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}' )          
          
          ## loop over all fields within 'g'
          ## (we are assuming no more RECORDs at this depth)
          for h in g.fields:

            if ( h.field_type=="RECORD" ):
              logging.error ( " RECORD found at {}>{}>{} ??? ", f.name, g.name, h.name )
              
            else:
              
              if ( h.mode=="REPEATED" ):
                numRF += 1
                qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                                  [f.name, g.name, h.name], [f.mode, g.mode, h.mode] )
                qr = bbqRunQuery ( bqclient, qs )
                sqr = bbqSummarizeQueryResults ( qr )
                if ( sqr[0] == 1 ):
                  print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10} always repeated {sqr[3]} time(s)' )
                else:
                  print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10}', sqr )                      
               

  if ( numRF < 1 ):
    print ( f' no REPEATED fields found in this table' )
             
  return ()


##--------------------------------------------------------------------------------------------------
##

def bbqRunQuery ( bqclient, qString, dryRun=False ):
  
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
    query_job = bqclient.query ( qString, job_config=job_config )
    logging.debug ( "    query job state: ", query_job.state )
  except:
    print ( "  FATAL ERROR: query submission failed " )
    return ( None )
  
  if ( not dryRun ):
    query_job = bqclient.get_job ( query_job.job_id )
    logging.debug ( ' Job {} is currently in state {}'.format ( query_job.job_id, query_job.state ) )
    while ( query_job.state != "DONE" ):
      time.sleep ( 1 )
      query_job = bqclient.get_job ( query_job.job_id )
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


##--------------------------------------------------------------------------------------------------
##

def bbqSummarizeQueryResults ( qr ):
  
  logging.debug ( "\n in bbqSummarizeQueryResults ... " )
  
  if ( not isinstance(qr, pd.DataFrame) ):
    return ( )
  else:
    
    ## first we want the number of rows in the result
    nVals = len(qr)
    logging.debug ( " # of rows in query results: {} ".format(len(qr)) )

    ## if we have at least one row, then ...
    if ( nVals > 0 ):
      logging.debug ( "\n", qr.head() )
      
      ## are there any null values?
      ## fnull = qr['f'].isnull()
      qin = qr [ qr['f'].isnull() ]
      if ( len(qin) == 1 ):
        numNull = qin['n'].iloc[0]
        logging.debug ( " NULL values found: ", numNull )
      else:
        numNull = 0
        logging.debug ( " ZERO null values found ... ")
      
      ## create a subset excluding the null row (if there is one) ...
      ## and if there's nothing left, then we're done here ...
      qnn = qr [ ~qr['f'].isnull() ]
      if ( len(qnn) < 1): return ( 0, numNull )
      
      ## number of unique non-null field values
      nVals = len(qnn)

      ## get the total number of non-null occurrences
      nTot = qnn['n'].sum()
      logging.debug ( " nTot = {} ".format(nTot) )

      ## get the most frequent non-null value and the # of times it occurs      
      cV = qnn['f'].iloc[0]
      cN = qnn['n'].iloc[0]      
      cF = float(cN)/float(nTot)
      logging.debug ( " most common value: {} {} {} ".format(cV,cN,cF) )
      
      ## get the 'minimum' and 'maximum' values ...
      minV = min ( qnn['f'] )
      maxV = max ( qnn['f'] )
      
      ## figure out how many unique values represent 50% and 90% of that total
      nT50 = int ( 0.50 * nTot )
      nT90 = int ( 0.90 * nTot )
      mTmp = 0
      iR = 0
      while ( mTmp < nT50 and iR < nVals ):
        mTmp += qnn['n'].iloc[iR]
        iR += 1
      mR50 = iR
      while ( mTmp < nT90 and iR < nVals ):
        mTmp += qnn['n'].iloc[iR]
        iR += 1
      mR90 = iR
      logging.debug ( " mTmp = {}   iR = {} ".format(mTmp,iR) )

      return ( nVals, numNull, nTot, cV, cN, cF, minV, maxV, mR50, mR90 )
      
    else: 
      return ( 0, 0 )

