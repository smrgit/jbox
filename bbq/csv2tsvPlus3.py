
import  argparse
import  csv
import  sys
import  time

##------------------------------------------------------------------------------

def cleanToken ( a ):

  double_chars = [ '||', ' |', '| ' ]
  for d in double_chars:
    if ( a.find(d) >= 0 ): 
      b = a.replace ( d, '|' )
      a = b
    if ( a.startswith(d) ): a = a[2:]
    if ( a.endswith(d) ): a = a[:-2]

  double_chars = [ '  ' ]
  for d in double_chars:
    if ( a.find(d) >= 0 ): 
      b = a.replace ( d, ' ' )
      a = b
    if ( a.startswith(d) ): a = a[2:]
    if ( a.endswith(d) ): a = a[:-2]


  spec_chars = [ '\\', '\r', '\n', '\t' ]
  for s in spec_chars:
    if ( a.find(s) >= 0 ): 
      b = a.replace ( s, '' )    
      a = b
    if ( a.startswith(s) ): a = a[1:]
    if ( a.endswith(s) ): a = a[:-1]

  if ( a.startswith(' ') ): a = a[1:]
  if ( a.endswith(' ') ): a = a[:-1]

  if ( a == '{}' ): a = ''
  if ( a == '[]' ): a = ''
  if ( a.lower() == 'na' ): a = ''
  if ( a.lower() == 'n/a' ): a = ''
  if ( a.lower() == 'unspecified' ): a = ''
  if ( a == ':' ):  a = ''

  return ( a )

##------------------------------------------------------------------------------

def main ( args ):

  print ( args )

  try:
    fhIn = open ( args.inputFile, 'r' )
  except:
    print ( " Failed to open input file <{}> ... EXITING ".format ( args.inputFile ) )
    sys.exit(-1)

  try:
    fhOut = open ( args.outputFile, 'w' )
  except:
    print ( " Failed to open output file ... EXITING " )
    sys.exit(-1)

  maxInt = sys.maxsize
  
  while True:
      # decrease the maxInt value by factor 10 
      # as long as the OverflowError occurs.
  
      try:
          csv.field_size_limit(maxInt)
          break
      except OverflowError:
          maxInt = int(maxInt/10)
  
  numIn = 0
  numOut = 0
  
  for u in csv.reader(fhIn):
    ## print " "
    ## print " -------------------------------------------------------------- "
    ## print " "
    ## print u
    ## print len(u)
    ## print " "
  
    ## check the number of tokens ...
    if ( numIn < 1 ):
      mCol = len(u)
    else:
      if ( mCol != len(u) ):
        print ( " (a) ERROR: invalid number of columns ??? " )
        print ( u )
        print ( len(u), mCol )
        sys.exit(-1)

    ## decide if we're going to skip this one or not ...
    if ( numIn%args.nSkip != 0 ):

      pass

    else:
   
      v = []
      for a in u:
        b = cleanToken ( a )
        v += [ b ]
      ## print v
      ## print len(v)
      ## print " "
    
      if ( mCol != len(v) ):
        print ( " (b) ERROR: invalid number of columns ??? " )
        print ( v )
        print ( len(u), len(v), mCol )
        sys.exit(-1)
    
      outLine = "\t".join(v)
      outLine += '\n'
      fhOut.write ( outLine )
     
      numOut = numOut + 1
      if ( numOut%10000 == 0 ): 
        print ( numIn, numOut, mCol )
        print ( v )
        print ( " " )

    ## increment the number of input rows handled
    numIn += 1

  ## completely done
  fhIn.close()
  fhOut.close()

  print ( " DONE !!! " )
  print ( numIn, numOut )
  

##------------------------------------------------------------------------------

if __name__ == '__main__':

  t0 = time.time()

  ## we need the following pieces of information:
  ##     inputFileName     -- required
  ##     outputFileName    -- required

  ##     sourceDataProject -- required
  ##     sourceBQdataset   -- required
  ##     sourceBQtable     -- required
  ##     outputDataProject -- if not specified, defaults to same as sourceDataProject
  ##     outputBQdataset   -- if not specified, defaults to same as sourceBQdataset
  ##     writeDisposition  -- default: WRITE_TRUNCATE
  ##     billingProject    -- if not specified, defaults to same as sourceDataProject

  parser = argparse.ArgumentParser()

  ## the first two arguments are required -- to fully specify the BQ table of interest
  parser.add_argument ( '-f',  '--inputFileName',  action='store', help='input CSV file', required=True, dest='inputFile', type=str )
  parser.add_argument ( '-o',  '--outputFileName', action='store', help='output TSV file', required=True, dest='outputFile', type=str )

  ## the next argument is optional, but can be used to handle only every Nth input line
  parser.add_argument ( '-n',  '--nSkip',  action='store', help='process only 1 out of N rows', dest='nSkip', type=int )

  args = parser.parse_args()

  if ( args.nSkip is None ): args.nSkip = 1
  if ( args.nSkip < 1 ): args.nSkip = 1

  main ( args )

  t1 = time.time()

  print ( ' --> time taken in seconds: {dt}'.format(dt=(t1-t0)) )

##------------------------------------------------------------------------------
