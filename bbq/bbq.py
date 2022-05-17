import logging
import pandas as pd
import sys
import time

from google.cloud import bigquery

##--------------------------------------------------------------------------------------------------
## this function builds a query to get some basic information about a single
## field which is NOT of type RECORD, in the specified table

## NB: the field of interest can be up to 4 layers 'deep', with a parent, a 'grandparent', 
## and a 'great-grandparent' in which case the names are all contained in the input 
## fNameList, with the child at the end of the list

## TODO: if a field is *all* Nulls, it will not show up in the output dataframe -- FIXME

def bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, fNameList, fTypeList, fModeList ):
  
  print ( ' ... in bbqBuildFieldContentsQuery ... ' )

  logging.debug ( projectName, datasetName, tableName )
  logging.debug ( fNameList, len(fNameList) )
  
  fdepth = len(fNameList)
  
  ## print ( ' in bbqBuildFieldContentsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
  
  if ( fdepth == 1 ):
    fName = fNameList[0]
    fMode = fModeList[0]

    print ( '     ... ', fName, fMode )
    
    if ( fMode=="REPEATED" ):
      ## construct a query for a REPEATED field
      qString = """
        WITH t1 AS ( SELECT f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{fName} AS f )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)      
    else:
      ## construct a query for a non-REPEATED field
      qString = """
        WITH t1 AS ( SELECT `{fName}` AS f FROM `{projectName}.{datasetName}.{tableName}` AS t )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)
  
  elif ( fdepth == 2 ):
    pName = fNameList[0]
    fName = fNameList[1]
    
    pMode = fModeList[0]
    fMode = fModeList[1]
    
    print ( '     ... ', fName, fMode, pName, pMode )

    if ( fMode=="REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{pName} AS u, u.{fName} AS f )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)
    elif ( pMode=="REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT u.`{fName}` AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{pName} AS u )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(pName=pName, fName=fName, projectName=projectName, datasetName=datasetName, tableName=tableName)       
    else:
      qString = """
        WITH t1 AS ( SELECT `{pName}`.`{fName}` AS f FROM `{projectName}.{datasetName}.{tableName}` AS t )
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

    print ( '     ... ', f, fMode, p, pMode, gp, gpMode )

    if ( fMode=="REPEATED" and pMode=="REPEATED" and gpMode=="REPEATED" ):
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT w AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName} AS v, v.{fName} AS w )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 

    elif ( fMode=="REPEATED" and pMode=="NULLABLE" and gpMode=="NULLABLE" ):
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT u.`{pName}`.`{fName}` AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 
    
    elif ( fMode=="REPEATED" and pMode=="NULLABLE" ):
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT v AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName}.{fName} AS v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 

    elif ( pMode=="REPEATED" ):
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT v.`{fName}` AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u, u.{pName} AS v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 
    else:
      ## print ( gp, p, f, fMode )
      qString = """
        WITH t1 AS ( SELECT u.`{pName}`.`{fName}` AS f FROM `{projectName}.{datasetName}.{tableName}` AS t, t.{gpName} AS u )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 
      
  elif ( fdepth == 4 ):

    ggp = fNameList[0]
    gp = fNameList[1]
    p = fNameList[2]
    f = fNameList[3]

    ggpMode = fModeList[0]
    gpMode = fModeList[1]
    pMode = fModeList[2]
    fMode = fModeList[3]

    print ( '     ... ', f, fMode, p, pMode, gp, gpMode, ggp, ggpMode )

    if ( fMode=="NULLABLE" and pMode=="REPEATED" and gpMode=="NULLABLE" and ggpMode=="REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT v.`{fName}` AS f 
          FROM `{projectName}.{datasetName}.{tableName}` AS t, 
            t.{ggpName} AS u, u.{gpName}.{pName} AS v )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(ggpName=ggp, gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 

    ## ['variants', 'transcripts', 'ensembl', 'consequence'] ['RECORD', 'RECORD', 'RECORD', 'STRING'] ['REPEATED', 'NULLABLE', 'REPEATED', 'REPEATED'] 4
    elif ( fMode=="REPEATED" and pMode=="REPEATED" and gpMode=="NULLABLE" and ggpMode=="REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT w AS f 
          FROM `{projectName}.{datasetName}.{tableName}` AS t, 
            t.{ggpName} AS u, 
            u.{gpName}.{pName} AS v,
            v.{fName} AS w )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(ggpName=ggp, gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 

    elif ( fMode=="NULLABLE" and pMode=="REPEATED" and gpMode=="REPEATED" and ggpMode=="REPEATED" ):
      qString = """
        WITH t1 AS ( SELECT w.`{fName}` AS f 
          FROM `{projectName}.{datasetName}.{tableName}` AS t, 
            t.{ggpName} AS u, 
            u.{gpName}  AS v,
            v.{pName}   AS w )
        SELECT f, COUNT(*) AS n FROM t1
        GROUP BY 1 ORDER BY 2 DESC, 1
      """.format(ggpName=ggp, gpName=gp, pName=p, fName=f, projectName=projectName, datasetName=datasetName, tableName=tableName)                 

    else:
      print ( ' I do not know how to handle this case yet... ' )
      print ( ' in bbqBuildFieldContentsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
      qString = "TODO"

  else:
    print ( ' I do not know how to handle this case yet... ' )
    print ( ' in bbqBuildFieldContentsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
    qString = "TODO"
  
  return ( qString )


##--------------------------------------------------------------------------------------------------
## this function builds a query to get some basic information about a single
## field which is of type RECORD

def bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, fNameList, fTypeList, fModeList ):
  
  print ( ' ... in bbqBuildRepeatedFieldsQuery ... ' )

  logging.debug ( projectName, datasetName, tableName )
  logging.debug ( fNameList, len(fNameList) )
  
  fdepth = len(fNameList)

  ## print ( ' in bbqBuildRepeatedFieldsQuery ... ', fNameList, fTypeList, fModeList, fdepth)
  
  if ( fdepth == 1 ):
    fName = fNameList[0]
    fMode = fModeList[0]
    
    if ( fMode=="REPEATED" ):
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

    if ( fMode=="REPEATED" ):
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
      qString = """
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

  print ( ' ... in bbqCheckQueryResults ... ' )

  logging.debug ( "\n in bbqCheckQueryResults ... " )
  logging.debug ( "         ", type(qr) )
  if ( not isinstance(qr, pd.DataFrame) ):
    logging.error ( " in bbqCheckQueryResults ... invalid results! " )
    sys.exit(-1)
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

def bbqExploreFieldContents ( bqclient, 
                              projectName, datasetName, tableName, 
                              excludedNames, excludedTypes,
                              verbose=True ):

  print ( ' ... in bbqExploreFieldContents ... ' )

  ## initialize a dataframe for the results
  resultsColumns = [ 'field_name', 'field_type', 'field_mode', 'n_fields', 'comment',
                     'n_vals', 'n_null', 'n_blank', 'n_total', 'common_val', 'common_n', 'common_f',
                     'min_val', 'max_val', 'minR50', 'minR90' ]
  rdf = pd.DataFrame(columns=resultsColumns)

  ## get a pointer to the BQ table ...
  dataset_ref = bqclient.dataset ( datasetName, project=projectName)
  table_ref = dataset_ref.table ( tableName )
  table = bqclient.get_table ( table_ref )
  
  ## outer loop over all fields in schema
  for f in table.schema:  

    ## if this field is a RECORD, then dig down ...
    if ( f.field_type=="RECORD" ):

      ## print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} [{len(f.fields)}] ' )
      if ( verbose ): print ( '{name:28}  {field_type:10}  {mode:10} [{n}]'.format(name=f.name, field_type=f.field_type, mode=f.mode, n=len(f.fields)) )
      rdf = rdf.append ( {'field_name': f.name, 'field_type': f.field_type, 'field_mode':f.mode, 'n_fields':len(f.fields)}, ignore_index=True )

      ## loop over all fields within 'f'
      for g in f.fields:     

        ## if this field is also a RECORD, dig further ...
        if ( g.field_type=="RECORD" ):

          ## print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} [{len(g.fields)}] ' )    
          if ( verbose ): print ( '    > {name:22}  {field_type:10}  {mode:10} [{n}]'.format(name=g.name, field_type=g.field_type, mode=g.mode, n=len(g.fields)) )    
          rdf = rdf.append ( {'field_name': f.name+'.'+g.name, 'field_type': g.field_type, 'field_mode':g.mode, 'n_fields':len(g.fields)}, ignore_index=True )

          ## loop over all fields within 'g'
          for h in g.fields:

            ## if this field is also a RECORD, dig further ...
            if ( h.field_type=="RECORD" ):

              ## print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10} [{len(h.fields)}] ' )    
              if ( verbose ): print ( '        > {name:18}  {field_type:10}  {mode:10} [{n}]'.format(name=h.name, field_type=h.field_type, mode=h.mode, n=len(h.fields)) )    
              rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name, 'field_type': h.field_type, 'field_mode':h.mode, 'n_fields':len(h.fields)}, ignore_index=True )

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
                      if ( len(sqr) > 2 ): 
                          ## print ( f'            > {j.name:14}  {j.field_type:10}  {j.mode:10}', sqr )   
                          if ( verbose ): print ( '            > {name:14}  {field_type:10}  {mode:10} {sqr}'.format(name=j.name, field_type=j.field_type, mode=j.mode, sqr=sqr) )   
                          rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name+'.'+j.name, 
                                        'field_type': j.field_type, 'field_mode':j.mode, 'n_fields':len(j.fields),
                                        'comment':'',
                                        'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                                        'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                                        'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )
                  else:
                    ## print ( f'        > {j.name:18}  {j.field_type:10}  {j.mode:10} (this field was excluded)' )
                    if ( verbose ): print ( '        > {name:14}  {field_type:10}  {mode:10} (this field was excluded)'.format(name=j.name, field_type=j.field_type, mode=j.mode) )   
                    rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name+'.'+j.name, 
                                  'field_type': j.field_type, 'field_mode':j.mode, 'n_fields':len(j.fields),
                                  'comment':'this field was excluded'}, ignore_index=True )
                
                    
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
                  if ( len(sqr) > 2 ): 
                      ## print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10}', sqr )   
                      if ( verbose ): print ( '        > {name:18}  {field_type:10}  {mode:10} {sqr}'.format(name=h.name, field_type=h.field_type, mode=h.mode, sqr=sqr) )   
                      rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name, 
                                    'field_type': h.field_type, 'field_mode':h.mode, 'n_fields':len(h.fields),
                                    'comment':'',
                                    'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                                    'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                                    'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )
              else:
                ## print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10} (this field was excluded)' )
                if ( verbose ): print ( '        > {name:18}  {field_type:10}  {mode:10} (this field was excluded)'.format(name=h.name, field_type=h.field_type, mode=h.mode) )
                rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name, 
                              'field_type': h.field_type, 'field_mode':h.mode, 'n_fields':len(h.fields),
                              'comment':'this field was excluded'}, ignore_index=True )
                
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
              if ( len(sqr) > 2 ): 
                  ## print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}', sqr )
                  if ( verbose ): print ( '    > {name:22}  {field_type:10}  {mode:10} {sqr}'.format(name=g.name, field_type=g.field_type, mode=g.mode, sqr=sqr) )
                  rdf = rdf.append ( {'field_name': f.name+'.'+g.name, 
                                'field_type': g.field_type, 'field_mode':g.mode, 'n_fields':len(g.fields),
                                'comment':'',
                                'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                                'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                                'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )
          else:
            ## print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} (this field was excluded)' )
            if ( verbose ): print ( '    > {name:22}  {field_type:10}  {mode:10} (this field was excluded)'.format(name=g.name, field_type=g.field_type, mode=g.mode) )
            rdf = rdf.append ( {'field_name': f.name+'.'+g.name, 
                          'field_type': g.field_type, 'field_mode':g.mode, 'n_fields':len(g.fields),
                          'comment':'this field was excluded'}, ignore_index=True )

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
          if ( len(sqr) > 2 ): 
              ## print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}', sqr )
              if ( verbose ): print ( '{name:28}  {field_type:10}  {mode:10} {sqr}'.format(name=f.name, field_type=f.field_type, mode=f.mode, sqr=sqr) )
              rdf = rdf.append ( {'field_name': f.name, 
                            'field_type': f.field_type, 'field_mode':f.mode, 'n_fields':len(f.fields),
                            'comment':'',
                            'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                            'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                            'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )
      else:
        ## print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} (this field was excluded)' )
        if ( verbose ): print ( '{name:28}  {field_type:10}  {mode:10} (this field was excluded)'.format(name=f.name, field_type=f.field_type, mode=f.mode) )
        rdf = rdf.append ( {'field_name': f.name, 
                      'field_type': f.field_type, 'field_mode':f.mode, 'n_fields':len(f.fields),
                      'comment':'this field was excluded'}, ignore_index=True )
          
  return ( rdf )


##--------------------------------------------------------------------------------------------------
##

def bbqExploreRepeatedFields ( bqclient, 
                               projectName, datasetName, tableName,
                               verbose=True ):

  print ( ' ... in bbqExploreRepeatedFields ... ' )

  ## initialize a dataframe for the results
  resultsColumns = [ 'field_name', 'field_type', 'field_mode', 'n_repeats', 'comment',
                     'n_vals', 'n_null', 'n_blank', 'n_total', 'common_val', 'common_n', 'common_f',
                     'min_val', 'max_val', 'minR50', 'minR90' ]
  rdf = pd.DataFrame(columns=resultsColumns)

  ## get a pointer to the BQ table ...
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
      print ( " A : ", qs )

      qr = bbqRunQuery ( bqclient, qs )
      print ( " B : ", qr )

      sqr = bbqSummarizeQueryResults ( qr )
      print ( " C : ", sqr )

      if ( sqr[0]==1 ):
        ## print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} always repeated {sqr[3]} time(s)' )
        if ( verbose ): print ( '{name:28}  {field_type:10}  {mode:10} always repeated {n} time(s)'.format(name=f.name, field_type=f.field_type, mode=f.mode, n=sqr[3]) )
        rdf = rdf.append ( {'field_name': f.name, 'field_type': f.field_type, 'field_mode':f.mode, 'n_repeats':sqr[3]}, ignore_index=True )
      else:  
        ## print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}', sqr )
        if ( verbose ): print ( '{name:28}  {field_type:10}  {mode:10} {sqr}'.format(name=f.name, field_type=f.field_type, mode=f.mode, sqr=sqr) )
        rdf = rdf.append ( {'field_name': f.name, 
                            'field_type': f.field_type, 'field_mode':f.mode, 'n_repeats':'variable',
                            'comment':'',
                            'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                            'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                            'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )
    
    ## if F is a RECORD ... dig further ...
    if ( f.field_type=="RECORD" ):
      
      if ( f.mode != "REPEATED" ):
        ## print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}' )
        if ( verbose ): print ( '{name:28}  {field_type:10}  {mode:10}'.format(name=f.name, field_type=f.field_type, mode=f.mode) )

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
          if ( len(sqr) == 0 ):
            print ( ' --> nothing back from bbqSummarizeQueryResults ? ' )
          else:
            if ( sqr[0] == 1 ):
              ## print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} always repeated {sqr[3]} time(s)' )
              if ( verbose ): print ( '    > {name:22}  {field_type:10}  {mode:10} always repeated {n} time(s)'.format(name=g.name, field_type=g.field_type, mode=g.mode, n=sqr[3]) )
              rdf = rdf.append ( {'field_name': f.name+'.'+g.name, 'field_type': g.field_type, 'field_mode':g.mode, 'n_repeats':sqr[3]}, ignore_index=True )
            else:
              ## print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}', sqr )        
              if ( verbose ): print ( '    > {name:22}  {field_type:10}  {mode:10} {sqr}'.format(name=g.name, field_type=g.field_type, mode=g.mode, sqr=sqr) )        
              rdf = rdf.append ( {'field_name': f.name+'.'+g.name, 
                              'field_type': g.field_type, 'field_mode':g.mode, 'n_repeats':'variable',
                              'comment':'',
                              'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                              'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                              'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )
        
        ## if G is a RECORD ... dig further ...
        if ( g.field_type=="RECORD" ):

          if ( g.mode != "REPEATED" ):
            ## print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}' )          
            if ( verbose ): print ( '    > {name:22}  {field_type:10}  {mode:10}'.format(name=g.name, field_type=g.field_type, mode=g.mode) )          
          
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
                ## print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10} always repeated {sqr[3]} time(s)' )
                if ( verbose ): print ( '        > {name:18}  {field_type:10}  {mode:10} always repeated {n} time(s)'.format(name=h.name, field_type=h.field_type, mode=h.mode, n=sqr[3]) )
                rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name, 
                                    'field_type': h.field_type, 'field_mode':h.mode, 'n_repeats':sqr[3]}, ignore_index=True )
              else:
                ## print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10}', sqr )        
                if ( verbose ): print ( '        > {name:18}  {field_type:10}  {mode:10} {sqr}'.format(name=h.name, field_type=h.field_type, mode=h.mode, sqr=sqr) )  
                rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name, 
                            'field_type': h.field_type, 'field_mode':h.mode, 'n_repeats':'variable',
                            'comment':'',
                            'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                            'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                            'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )

            ## if H is a RECORD ... dig further ...
            if ( h.field_type=="RECORD" ):

              if ( h.mode != "REPEATED" ):
                ## print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10}' )          
                if ( verbose ): print ( '        > {name:18}  {field_type:10}  {mode:10}'.format(name=h.name, field_type=h.field_type, mode=h.mode) )          

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
                    ## print ( f'            > {j.name:14}  {j.field_type:10}  {j.mode:10} always repeated {sqr[3]} time(s)' )
                    if ( verbose ): print ( '            > {name:14}  {field_type:10}  {mode:10} always repeated {n} time(s)'.format(name=j.name, field_type=j.field_type, mode=j.mode, n=sqr[3]) )
                    rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name+'.'+j.name, 
                                        'field_type': j.field_type, 'field_mode':j.mode, 'n_repeats':sqr[3]}, ignore_index=True )
                  else:
                    ## print ( f'            > {j.name:14}  {j.field_type:10}  {j.mode:10}', sqr )        
                    if ( verbose ): print ( '            > {name:14}  {field_type:10}  {mode:10} {sqr}'.format(name=j.name, field_type=j.field_type, mode=j.mode, sqr=sqr) )        
                    rdf = rdf.append ( {'field_name': f.name+'.'+g.name+'.'+h.name+'.'+j.name, 
                            'field_type': j.field_type, 'field_mode':j.mode, 'n_repeats':'variable',
                            'comment':'',
                            'n_vals':sqr[0], 'n_null':sqr[1], 'n_blank':sqr[2], 'n_total':sqr[3], 
                            'common_val':sqr[4], 'common_n':sqr[5], 'common_f':sqr[6],
                            'min_val':sqr[7], 'max_val':sqr[8], 'minR50':sqr[9], 'minR90':sqr[10]}, ignore_index=True )

                ## hope and pray that J is not a RECORD ...
                if ( j.field_type=="RECORD" ):
                  logging.error ( " RECORD found at {}>{}>{}>{} ??? ".format(f.name, g.name, h.name, j.name) )

  ## print ( rdf )
              
  if ( numRF < 1 ):
    ## print ( f' no REPEATED fields found in this table' )
    print ( ' no REPEATED fields found in this table' )
             
  return ( rdf )

##--------------------------------------------------------------------------------------------------
##

def bbqRunQuery ( bqclient, qString, dryRun=False ):

  print ( ' ... in bbqRunQuery ... ' )

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
    print ( '     submitting query ... ' )
    query_job = bqclient.query ( qString, job_config=job_config )
    logging.debug ( "    query job state: ", query_job.state )
    print ( '    query job state: ', query_job.state )
  except:
    print ( "  FATAL ERROR: query submission failed " )
    return ( None )
  
  if ( not dryRun ):
    query_job = bqclient.get_job ( query_job.job_id )
    logging.debug ( ' Job {} is currently in state {}'.format ( query_job.job_id, query_job.state ) )
    print ( ' Job {} is currently in state {}'.format ( query_job.job_id, query_job.state ) )
    while ( query_job.state != "DONE" ):
      time.sleep ( 1 )
      query_job = bqclient.get_job ( query_job.job_id )
      logging.debug ( ' Job {} is currently in state {}'.format ( query_job.job_id, query_job.state ) )
      print ( ' Job {} is currently in state {}'.format ( query_job.job_id, query_job.state ) )

  ## return results as a dataframe (or an empty dataframe for a dry-run) 
  if ( not dryRun ):
    ## try:
    if ( 1 ):
      print ( ' ... calling to_dataframe() ... ' )
      df = query_job.to_dataframe()
      print ( ' ... back from call ... ', type(df) )
      if ( query_job.total_bytes_processed==0 ):       
        logging.debug ( "    the results for this query were previously cached " )
      else:
        logging.debug ( "    this query processed {} bytes ".format(query_job.total_bytes_processed) )
    
      ## if ( len(df) < 1 ): logging.warning ( "  WARNING: this query returned NO results ")
    
      logging.debug ( " --> returning dataframe ... ", len(df) )
      return ( df )

##     except:
##       print ( "   FATAL ERROR: failed to get results as a data-frame ??? " )
##       print ( qString )
##       return ( None )
    
  else:
    logging.info ( "    if not cached, this query will process {} bytes ".format(query_job.total_bytes_processed) )
    ## return an empty dataframe
    return ( pd.DataFrame() )


##--------------------------------------------------------------------------------------------------
##

def bbqSummarizeQueryResults ( qr ):
  
  print ( ' ... in bbqSummarizeQueryResults ... ' )

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
      ## print ( qnn )
      if ( len(qnn) < 1): return ( 0, numNull )

      ## print ( " --> len(qnn) = ", len(qnn) )

      ## in addition to null rows, there might also be rows that contain *blank* strings ...
      try:
        qblank = qr [ qr['f'].str.strip().eq('') ]
        if ( len(qblank) > 0 ):
          numBlank = qblank['n'].sum()
          logging.debug ( " BLANK values found: ", numBlank )
          ## create a new subset excluding the blank row(s) ...
          qnn = qnn [ ~qnn['f'].str.strip().eq('') ]
        else:
          numBlank = 0
          logging.debug ( " ZERO blank values found ... ")
      except:
          numBlank = 0
          logging.debug ( " ZERO blank values found ... ")
     
      ## number of unique non-null field values
      nVals = len(qnn)

      ## get the total number of non-null occurrences
      nTot = qnn['n'].sum()
      logging.debug ( " nTot = {} ".format(nTot) )

      ## get the most frequent non-null value and the # of times it occurs      
      try:
          cV = qnn['f'].iloc[0]
      except:
          cV = '?'
      try:
          cN = qnn['n'].iloc[0]      
          cF = float(cN)/float(nTot)
      except:
          cN = 0
          cF = 0
      logging.debug ( " most common value: {} {} {} ".format(cV,cN,cF) )
      
      ## get the 'minimum' and 'maximum' values ...
      try:
          minV = min ( qnn['f'] )
          maxV = max ( qnn['f'] )
      except:
          minV = '?'
          maxV = '?'
      
      ## figure out how many unique values represent 50% and 90% of that total
      if ( nTot > 1 ):
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
      else:
          mR50 = 0
          mR90 = 0
          mTmp = 0
          iR = 0
      logging.debug ( " mTmp = {}   iR = {} ".format(mTmp,iR) )

      return ( nVals, numNull, numBlank, nTot, cV, cN, cF, minV, maxV, mR50, mR90 )
      
    else: 
      return ( 0, 0 )

