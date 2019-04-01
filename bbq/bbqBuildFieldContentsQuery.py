import logging
import pandas as pd

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

