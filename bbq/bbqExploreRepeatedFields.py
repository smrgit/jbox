def bbqExploreRepeatedFields ( projectName, datasetName, tableName ):

  dataset_ref = bqclient.dataset ( datasetName, project=projectName)
  table_ref = dataset_ref.table ( tableName )
  table = bqclient.get_table ( table_ref )
  
  numRF = 0
  
  ## outer loop over all fields in schema
  for f in table.schema:  

    if ( f.mode=="REPEATED" ):
      numRF += 1
      qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                        [f.name], [f.mode] )
      qr = bbqRunQuery ( bqclient, qs )
      sqr = bbqSummarizeQueryResults ( qr )
      if ( sqr[0]==1 ):
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10} always repeated {sqr[3]} time(s)' )
      else:  
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}', sqr )
    
    ## if this field is a RECORD, then dig down ...
    if ( f.field_type=="RECORD" ):
      
      if ( f.mode != "REPEATED" ):
        print ( f'{f.name:28}  {f.field_type:10}  {f.mode:10}' )

      ## loop over all fields within 'f'
      for g in f.fields:     

        if ( g.mode=="REPEATED" ):
          numRF += 1
          qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                            [f.name, g.name], [f.mode, g.mode] )
          qr = bbqRunQuery ( bqclient, qs )
          sqr = bbqSummarizeQueryResults ( qr )
          if ( sqr[0] == 1 ):
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10} always repeated {sqr[3]} time(s)' )
          else:
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}', sqr )        
        
        ## if this field is also a RECORD, dig further ...
        if ( g.field_type=="RECORD" ):

      
          if ( g.mode != "REPEATED" ):
            print ( f'    > {g.name:22}  {g.field_type:10}  {g.mode:10}' )          
          
          ## loop over all fields within 'g'
          ## (we are assuming no more RECORDs at this depth)
          for h in g.fields:

            if ( h.field_type=="RECORD" ):
              logging.error ( " RECORD found at {}>{}>{} ??? ", f.name, g.name, h.name )
              
            else:
              
              if ( h.mode=="REPEATED" ):
                numRF += 1
                qs = bbqBuildRepeatedFieldsQuery ( projectName, datasetName, tableName, 
                                                  [f.name, g.name, h.name], [f.mode, g.mode, h.mode] )
                qr = bbqRunQuery ( bqclient, qs )
                sqr = bbqSummarizeQueryResults ( qr )
                if ( sqr[0] == 1 ):
                  print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10} always repeated {sqr[3]} time(s)' )
                else:
                  print ( f'        > {h.name:18}  {h.field_type:10}  {h.mode:10}', sqr )                      
               

  if ( numRF < 1 ):
    print ( f' no REPEATED fields found in this table' )
             
  return ()

