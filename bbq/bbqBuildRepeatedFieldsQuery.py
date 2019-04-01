import logging
import pandas as pd

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

