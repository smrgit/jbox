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

def bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, fNameList, fTypeList, fModeList ):
  
  logging.debug ( projectName, datasetName, tableName )
  logging.debug ( fNameList, len(fNameList) )
  
  fdepth = len(fNameList)
  
  print ( ' in bbqBuildFieldContentsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
  
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

    print ( ' testing ... G ' )
    ## working on  ['variants', 'clinvar', 'orphanetIds'] ['REPEATED', 'REPEATED', 'REPEATED'] 3

    if ( fMode == "REPEATED" and pMode == "REPEATED" and gpMode == "REPEATED" ):
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT w AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName} AS v, v.{fName} AS w )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 
    
    elif ( fMode == "REPEATED" and pMode == "NULLABLE" ):
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT v AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName}.{fName} AS v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 

    elif ( pMode == "REPEATED" ):
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT v.{fName} AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName} AS v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 
    else:
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT u.{pName}.{fName} AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 
      
  else:
    print ( ' I do not know how to handle this case yet... ' )
    print ( ' in bbqBuildFieldContentsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
    qString = "TODO"
  
  return ( qString )


##--------------------------------------------------------------------------------------------------
## this function builds a query to get some basic information about a single
## field which is of type RECORD

def bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, fNameList, fTypeList, fModeList ):
  
  logging.debug ( projectName, datasetName, tableName )
  logging.debug ( fNameList, len(fNameList) )
  
  fdepth = len(fNameList)

  print ( ' in bbqBuildRepeatedFieldsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
  
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
      print ( '     should I even be getting here ??? (a) ' )
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
      ## construct a query for a non-REPEATED field
      print ( '     should I even be getting here ??? (b) ' )
      qString = "TODO ???"

  elif ( fdepth == 3 ):

    gpName = fNameList[0]
    pName = fNameList[1]
    fName = fNameList[2]

    gpMode = fModeList[0]
    pMode = fModeList[1]
    fMode = fModeList[2]

    if ( fMode=="REPEATED" and pMode=="NULLABLE" ):
      qString = """
        WITH t1 AS ( SELECT ARRAY_LENGTH(u.{pName}.{fName}) AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gpName, pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)      

    else:
      qString = """
        WITH t1 AS ( SELECT ARRAY_LENGTH(v.{fName}) AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName} as v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gpName, pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName) 

  elif ( fdepth == 4 ):
    ## ['variants', 'transcripts', 'ensembl', 'consequence'] ['RECORD', 'RECORD', 'RECORD', 'STRING'] ['REPEATED', 'NULLABLE', 'REPEATED', 'REPEATED']

    ggpName = fNameList[0]
    gpName = fNameList[1]
    pName = fNameList[2]
    fName = fNameList[3]

    ggpMode = fModeList[0]
    gpMode = fModeList[1]
    pMode = fModeList[2]
    fMode = fModeList[3]

    if ( fMode=="REPEATED" and pMode=="REPEATED" and gpMode=="NULLABLE" and ggpMode=="REPEATED" ):
      qString == """
        WITH t1 AS ( SELECT ARRAY_LENGTH(v.{fName}) AS f 
        FROM 
          `{projectName}.{datasetName}.{tableName}` AS t, 
          t.{ggpName} AS u, u.{gpName}.{pName} as v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(ggpName=ggpName, gpName=gpName, pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName) 

    else:
      print ( " do not know how to handle this one yet ... " )

  else:
    print ( ' getting in too DEEP !!! ' )
    print ( ' in bbqBuildRepeatedFieldsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
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
## there is probably a more elegant way to do this with a recursive function, but that will
## have to wait for another time ...
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
          for h in g.fields:

            ## if this field is also a RECORD, dig further ...
            if ( h.field_type=="RECORD" ):

              print ( f'    >    > {h.name:14}  {h.field_type:10}  {h.mode:10} [{len(h.fields)}] ' )    

              ## loop over all fields within 'h'
              for j in h.fields:

                ## if this field is also a RECORD, bail!
                if ( j.field_type=="RECORD" ):
                  logging.error ( " RECORD found at {}>{}>{}>{} ??? ".format(f.name, g.name, h.name, j.name) )
                ## if j is NOT a RECORD:
                else:

                  if ( j.name not in excludedNames and j.field_type not in excludedTypes ):
                    ## build query to get summary information about field 'h' ...
                    qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, 
                                                     [f.name, g.name, h.name, j.name], 
                                                     [f.field_type, g.field_type, h.field_type, j.field_type],
                                                     [f.mode, g.mode, h.mode, j.mode] )
                    ## print ( qs )
                    qr = bbqRunQuery ( bqclient, qs )
                    if ( not bbqCheckQueryResults ( qr ) ):
                      print ( " Query failed ??? " )
                      print ( qs )
                    else:
                      sqr = bbqSummarizeQueryResults ( qr )
                      if ( len(sqr) > 2 ): print ( f'            > {j.name:14}  {j.field_type:10}  {j.mode:10}', sqr )   
                  else:
                    print ( f'        > {j.name:18}  {j.field_type:10}  {j.mode:10} (this field was excluded)' )
                
                    
            ## if h is NOT a RECORD:  
            else:
            
              if ( h.name not in excludedNames and h.field_type not in excludedTypes ):                
                ## build query to get summary information about field 'h' ...
                qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, 
                                                 [f.name, g.name, h.name], 
                                                 [f.field_type, g.field_type, h.field_type],
                                                 [f.mode, g.mode, h.mode] )
                ## print ( qs )
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
                                             [f.name, g.name], 
                                             [f.field_type, g.field_type],
                                             [f.mode, g.mode] )
            ## print ( qs )
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
        qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, 
                                          [f.name], [f.field_type], [f.mode] )
        ## print ( qs )
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

  ## outer loop over all fields F in TABLE schema
  for f in table.schema:  

    ## if F is a REPEATED field ...
    if ( f.mode=="REPEATED" ):
      numRF += 1
      qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                        [f.name], [f.field_type], [f.mode] )
      qr = bbqRunQuery ( bqclient, qs )
      sqr = bbqSummarizeQueryResults ( qr )
      if ( sqr[0]==1 ):
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} always repeated {sqr[3]} time(s)' )
      else:  
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}', sqr )
    
    ## if F is a RECORD ... dig further ...
    if ( f.field_type=="RECORD" ):
      
      if ( f.mode != "REPEATED" ):
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}' )

      ## next loop over all fields G in record F:
      for g in f.fields:     

        ## if G is a REPEATED field ...
        if ( g.mode=="REPEATED" ):
          numRF += 1
          qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                            [f.name, g.name], 
                                            [f.field_type, g.field_type],
                                            [f.mode, g.mode] )
          qr = bbqRunQuery ( bqclient, qs )
          sqr = bbqSummarizeQueryResults ( qr )
          if ( sqr[0] == 1 ):
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} always repeated {sqr[3]} time(s)' )
          else:
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}', sqr )        
        
        ## if G is a RECORD ... dig further ...
        if ( g.field_type=="RECORD" ):

          if ( g.mode != "REPEATED" ):
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}' )          
          
          ## next loop over all fields H in record G:
          for h in g.fields:

            ## if H is a REPEATED field ...
            if ( h.mode=="REPEATED" ):
              numRF += 1
              qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName,
                                                 [f.name, g.name, h.name],
                                                 [f.field_type, g.field_type, h.field_type],
                                                 [f.mode, g.mode, h.mode] )
              qr = bbqRunQuery ( bqclient, qs )
              sqr = bbqSummarizeQueryResults ( qr )
              if ( sqr[0] == 1 ):
                print ( f'    > {h.name:22}  {h.field_type:10}  {h.mode:10} always repeated {sqr[3]} time(s)' )
              else:
                print ( f'    > {h.name:22}  {h.field_type:10}  {h.mode:10}', sqr )        

            ## if H is a RECORD ... dig further ...
            if ( h.field_type=="RECORD" ):

              if ( h.mode != "REPEATED" ):
                print ( f'    > {h.name:22}  {h.field_type:10}  {h.mode:10}' )          

              ## next loop over all fields J in record H:
              for j in h.fields:

                ## if J is a REPEATED field ...
                if ( j.mode=="REPEATED" ):
                  numRF += 1
                  qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName,
                                                     [f.name, g.name, h.name, j.name],
                                                     [f.field_type, g.field_type, h.field_type, j.field_type],
                                                     [f.mode, g.mode, h.mode, j.mode] )
                  qr = bbqRunQuery ( bqclient, qs )
                  sqr = bbqSummarizeQueryResults ( qr )
                  if ( sqr[0] == 1 ):
                    print ( f'    > {j.name:22}  {j.field_type:10}  {j.mode:10} always repeated {sqr[3]} time(s)' )
                  else:
                    print ( f'    > {j.name:22}  {j.field_type:10}  {j.mode:10}', sqr )        

                ## hope and pray that J is not a RECORD ...
                if ( j.field_type=="RECORD" ):
                  logging.error ( " RECORD found at {}>{}>{}>{} ??? ".format(f.name, g.name, h.name, j.name) )
              
            else:
              
              if ( h.mode=="REPEATED" ):
                numRF += 1
                qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                                  [f.name, g.name, h.name], 
                                                  [f.field_type, g.field_type, h.field_type],
                                                  [f.mode, g.mode, h.mode] )
                qr = bbqRunQuery ( bqclient, qs )
                sqr = bbqSummarizeQueryResults ( qr )
                if ( sqr[0] == 1 ):
                  print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10} always repeated {sqr[3]} time(s)' )
                else:
                  print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10}', sqr )                      
               

  if ( numRF < 1 ):
    print ( f' no REPEATED fields found in this table' )
             
  return ()


          ## (we are assuming no more RECORDs at this depth)
##--------------------------------------------------------------------------------------------------
##

def bbqRunQuery ( bqclient, qString, dryRun=False ):

  if ( qString.upper().find("SELECT") < 0 ):
    logging.error ( " >>>> bad query string <<< " )
    return ( None )
  
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
    try:
      df = query_job.to_dataframe()
      if ( query_job.total_bytes_processed==0 ):       
        logging.debug ( "    the results for this query were previously cached " )
      else:
        logging.debug ( "    this query processed {} bytes ".format(query_job.total_bytes_processed) )
    
      ## if ( len(df) < 1 ): logging.warning ( "  WARNING: this query returned NO results ")
    
      logging.debug ( " --> returning dataframe ... ", len(df) )
      return ( df )

    except:
      print ( "   FATAL ERROR: failed to get results as a data-frame ??? " )
      print ( qString )
      return ( None )
    
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

