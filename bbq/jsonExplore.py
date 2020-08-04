
import  argparse
import  csv
import  json
import  pandas as pd
import  sys
import  time

##------------------------------------------------------------------------------
##------------------------------------------------------------------------------

def main ( args ):

  print ( " " )
  print ( " in jsonExplore ... ", args )
  print ( " " )

  try:
    fhIn = open ( args.inputFile, 'r' )
    print ( " Opened input file <{}> ".format ( args.inputFile ) )
  except:
    print ( " Failed to open input file <{}> ... EXITING ".format ( args.inputFile ) )
    sys.exit(-1)

  try:
      ## print ( " reading input JSON ... " )
      df = pd.read_json ( fhIn, orient='records', lines=True, \
                      convert_dates=False, keep_default_dates=False )
  except:
    print ( " failed to read input JSON data ??? " )
    sys.exit(-1)

##   print ( " " )
##   print ( " back from read_json ... " )
##   print ( df.columns, df.shape )
##   print ( df.head() )
##   print ( df.describe() )

  originalShape = df.shape

  ## test that the field we are supposed to explore actually exists ...
  if ( args.exploreFieldName not in df.columns ):
    print ( " ... field to be explored <%s> was not found ... " % args.exploreFieldName )
    print ( df.columns )
    return

##   print ( " " )
##   print ( " " )
##   print ( " creating dfX ... " )
##   print ( " " )

  dfX = df.loc[:, [args.exploreFieldName]]
##   print ( dfX.columns, dfX.shape )
##   print ( dfX.head() )
##   print ( dfX.describe() )

  keyCounts = {}
  keyValues = {}

  jt = dfX.to_json ( orient='records', lines=True )
  rows = jt.splitlines ( keepends=False )
  for r in rows:
    d = json.loads(r)
    ## print ( " d : ", d )

    d2 = d[args.exploreFieldName]
    if ( d2 is not None ): 
      ## print ( " --> d2 : ", type(d2), d2 )

      d3 = json.loads(d2)
      ## print ( " --> d3 : ", type(d3), d3 )

      if ( isinstance(d3,dict) ):
        ## print ( " --> is dict d3 : ", d3 )
        for k in d3:
          if ( k not in keyCounts ): 
            keyCounts[k] = 0
            keyValues[k] = {}
          zVal = d3[k]
          if ( zVal ):
            if ( len(str(zVal)) > 0 ):
              keyCounts[k] += 1
              if ( zVal not in keyValues[k] ):
                keyValues[k][zVal] = 1
              else:
                keyValues[k][zVal] += 1

  print ( " " )
  print ( " " )
  print ( " fields found in JSON : " )

  outString = "num_occur\tkey_name\tnum_values\tvalues"
  print ( "%s" % outString )

  for k in keyCounts:
    numVals = len(keyValues[k])
    outString = "%d\t%s\t%d\t" % ( keyCounts[k], k, numVals )

    if ( numVals <= 3 ): 
      outString += repr(keyValues[k])

    else:

        bigC = 0
        for v in keyValues[k]:
            if ( bigC < keyValues[k][v] ): 
                bigC = keyValues[k][v]
        ## {'stable': 1122, 'low': 10, 'high': 35}

        outString += "{"
        maxC = bigC
        minC = 0.90 * maxC
        numOut = 0
        while ( len(outString)<150 and numOut<numVals ):

          ## print ( "       ... ", len(outString), minC, maxC, numOut, numVals )
          
          for v in keyValues[k]:
              if ( keyValues[k][v] > minC and keyValues[k][v] <= maxC  ):
                  outString += "'%s': %d, " % ( v, keyValues[k][v] )
                  numOut += 1

          maxC = minC
          minC = 0.90 * maxC

        if ( len(outString) > 4 ): 
          if ( numOut < numVals ):
            outString += "...}"
          else:
            outString = outString[:-2] + "}"

    print ( "%s" % outString )
    ## print ( " " )

  print ( " " )
  print ( " " )
  ## cannot just use to_json because we need to drop the 'null' values...
  ## dfX.to_json ( fhOut, orient='records', lines=True )


##------------------------------------------------------------------------------

if __name__ == '__main__':

  t0 = time.time()

  parser = argparse.ArgumentParser()

  ## the first two arguments are required -- to fully specify the BQ table of interest
  parser.add_argument ( '-f',  '--inputFileName',  action='store', help='input JSON file', required=True, dest='inputFile', type=str )
  parser.add_argument ( '-x',  '--exploreField',   action='store', help='name of fields to be explored', required=True, dest='exploreFieldName', type=str )

  args = parser.parse_args()

  main ( args )

  t1 = time.time()

  print ( ' --> time taken in seconds: {dt}'.format(dt=(t1-t0)) )

##------------------------------------------------------------------------------
