
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
  print ( " in jsonExplode ... ", args )
  print ( " " )

  try:
    fhIn = open ( args.inputFile, 'r' )
    print ( " Opened input file <{}> ".format ( args.inputFile ) )
  except:
    print ( " Failed to open input file <{}> ... EXITING ".format ( args.inputFile ) )
    sys.exit(-1)

  try:
      print ( " reading input JSON ... " )
      df = pd.read_json ( fhIn, orient='records', lines=True, \
                      convert_dates=False, keep_default_dates=False, dtype={} )
  except:
    print ( " failed to read input JSON data ??? " )
    sys.exit(-1)

  print ( " " )
  print ( " back from read_json ... " )
  print ( df.columns, df.shape )
  print ( df.head() )
  ## print ( df.describe() )

  originalShape = df.shape

  ## test that the field we are supposed to explode actually exists ...
  if ( args.explodeFieldName not in df.columns ):
    print ( " ... field to be exploded <%s> was not found ... " % args.explodeFieldName )
    print ( df.columns )
    dfX = df
  else:
    dfX = df.explode(args.explodeFieldName)
    print ( " " )
    print ( " after exploding <%s> column ... " % args.explodeFieldName )
    print ( dfX.columns, dfX.shape )
    print ( dfX.describe() )

  postExplodeShape = dfX.shape

  ## drop duplicates if there are any...
  dfX.drop_duplicates(inplace=True)
  print ( " " )
  print ( " after dropping duplicates ... " )
  print ( dfX.columns, dfX.shape )
  print ( dfX.describe() )

  postDropShape = dfX.shape

  try:
    fhOut = open ( args.outputFile, 'w' )
  except:
    print ( " Failed to open output file ... EXITING " )
    sys.exit(-1)

  ## cannot just use to_json because we need to drop the 'null' values...
  ## dfX.to_json ( fhOut, orient='records', lines=True )

  j1 = dfX.to_json ( orient='records', lines=True )
  rows = j1.splitlines ( keepends=False )
  for r in rows:
    ## print ( " " )
    ## print ( " r : ", r )
    d = json.loads(r)
    ## print ( " --> d : ", d )
    if ( isinstance(d,dict) ):
      sd = {}
      for k in d:
        ## print ( "     k : ", k )
        if ( d[k] ):
          if ( len(str(d[k])) > 0 ): 
            z = d[k]
            ## print ( k, type(d[k]), d[k] )
            if ( isinstance(z,str) ): sd[k] = z
            elif ( isinstance(z,float) ):
                iz = int(z)
                if ( abs(z-float(iz)) < 0.01 ): 
                  sd[k] = str(iz)
                else:
                  sd[k] = str(z)
            elif ( isinstance(z,int) ):
                iz = int(z)
                sd[k] = str(iz)
            else:
                ## print ( " NEVER SHOULD GET HERE SHOULD WE ??? !!! " )
                sys.exit(-1)
      ## print ( " --> sd : ", sd )
      fhOut.write ( json.dumps(sd)+"\n" )

  fhOut.close()

  print ( " " )
  print ( " finished exploding <%s> " % args.explodeFieldName )
  print ( "      original data shape .... ", originalShape )
  print ( "      after exploding ........ ", postExplodeShape )
  print ( "      after dropping dup's ... ", postDropShape )
  print ( " " )

##------------------------------------------------------------------------------

if __name__ == '__main__':

  t0 = time.time()

  ## we need the following pieces of information:
  ##     inputFileName     -- required
  ##     outputFileName    -- required

  parser = argparse.ArgumentParser()

  ## the first two arguments are required -- to fully specify the BQ table of interest
  parser.add_argument ( '-f',  '--inputFileName',  action='store', help='input JSON file', required=True, dest='inputFile', type=str )
  parser.add_argument ( '-x',  '--explodeField',   action='store', help='name of fields to be exploded', required=True, dest='explodeFieldName', type=str )
  parser.add_argument ( '-o',  '--outputFileName', action='store', help='output JSON file', required=True, dest='outputFile', type=str )

  args = parser.parse_args()

  main ( args )

  t1 = time.time()

  print ( ' --> time taken in seconds: {dt}'.format(dt=(t1-t0)) )

##------------------------------------------------------------------------------
