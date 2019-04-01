
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

