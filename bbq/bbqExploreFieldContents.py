
def bbqExploreFieldContents ( projectName, datasetName, tableName, excludedNames, excludedTypes ):

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
          ## (we are assuming no more RECORDs at or beyond this depth)
          for h in g.fields:

            if ( h.field_type=="RECORD" ):
              logging.error ( " RECORD found at {}>{}>{} ??? ", f.name, g.name, h.name )
              
            else:
            
              if ( h.name not in excludedNames and h.field_type not in excludedTypes ):                
                ## build query to get summary information about field 'h' ...
                qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, 
                                                 [f.name, g.name, h.name], [f.mode, g.mode, h.mode] )
                print ( qs )
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
                                             [f.name, g.name], [f.mode, g.mode] )
            print ( qs )
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
        qs = bbqBuildFieldContentsQuery ( projectName, datasetName, tableName, [f.name], [f.mode] )
        print ( qs )
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

