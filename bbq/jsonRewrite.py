
import  argparse
import  csv
import  json
import  pandas as pd
import  sys
import  time

##------------------------------------------------------------------------------

def myToJson ( fhOut, chunkDf ):

    ## chunkDf.to_json ( fhOut, orient='records', lines=True )

    j1 = chunkDf.to_json(orient='records',lines=True)
    ## print ( " got j1 ... " )
    ## print ( "     type   : ", type(j1) )
    ## print ( "     length : ", len(j1) )

    rows = j1.splitlines(keepends=False)
    ## print ( len(rows) )

    for r in rows:
        ## print ( r )
        try:
            ## d = eval(r)
            d = json.loads(r)
        except:
            try:
                d = json.loads(r)
            except:
                print ( " WHY DID THIS HAPPEN in myToJson ??? ", len(rows) )
                print ( r )
                sys.exit(-1)

        if ( not isinstance(d,dict) ):
            print ( " FATAL ERROR in myToJson ??? !!! " )
            print ( r )
            print ( d )
            sys.exit(-1)

        ## print ( len(d), d )

        if ( 0 ):
            ## ----------------------------------------------
            ## DO NOT NEED TO DO ANY OF THIS IN THIS CASE ...
            ## ----------------------------------------------
            sd = {}
            ## 11-apr-2020: added sorted() below
            for k in sorted(d):
                print ( " checking on key <%s> " % k )
                print ( "       ", d[k] )
                if ( isinstance(d[k],str) ): d[k] = d[k].strip()
                try:
                    if ( len(d[k]) == 0 ):
                        print ( " (a) skipping key <%s> " % k )
                        pass
                    else:
                        sd[k] = d[k]
                except:
                    try:
                        if ( d[k] is None ):
                            print ( " (b) skipping key <%s> " % k )
                            pass
                    except:
                        print ( " WTH is going on here ??? in myToJson ... " )
                        print ( len(d), d )
                        print ( k )
                        print ( d[k] )
                        sys.exit(-1)
    
            if ( len(sd) < len(d) ): print ( len(sd), sd )
    
            sss = json.dumps(sd,sort_keys=True)
            fhOut.write ( json.dumps(sd,sort_keys=True)+"\n" )

        else:
            sss = json.dumps(d,sort_keys=True)
            fhOut.write ( json.dumps(d,sort_keys=True)+"\n" )

    return ()

##------------------------------------------------------------------------------

def main ( args ):

    print ( " " )
    print ( " in jsonRewrite ... ", args )
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
                        convert_dates=False, keep_default_dates=False, dtype={} )
    except:
        print ( " failed to read input JSON data ??? " )
        sys.exit(-1)

    print ( " " )
    print ( " back from read_json ... " )
    print ( df.columns, df.shape )
    print ( df.head() )
    print ( df.describe() )

    originalShape = df.shape

    try:
        fhOut = open ( args.outputFile, 'w' )
        print ( " Opened output file <{}> ".format ( args.outputFile ) )
    except:
        print ( " Failed to open output file <{}> ... EXITING ".format ( args.outputFile ) )
        sys.exit(-1)

    print ( " calling myToJson ... " )
    myToJson ( fhOut, df )

##------------------------------------------------------------------------------

if __name__ == '__main__':

  t0 = time.time()

  parser = argparse.ArgumentParser()

  ## the first two arguments are required -- to fully specify the BQ table of interest
  parser.add_argument ( '-f',  '--inputFileName',  action='store', help='input JSON file', required=True, dest='inputFile', type=str )
  parser.add_argument ( '-o',  '--outputFileName', action='store', help='output JSON file', required=True, dest='outputFile', type=str )

  args = parser.parse_args()

  main ( args )

  t1 = time.time()

  print ( ' --> time taken in seconds: {dt}'.format(dt=(t1-t0)) )

##------------------------------------------------------------------------------
