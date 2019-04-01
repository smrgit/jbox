import logging
import pandas as pd

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

