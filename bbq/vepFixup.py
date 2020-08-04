
import  argparse
import  csv
import  json
import  pandas as pd
import  sys
import  time

##------------------------------------------------------------------------------

def parseHGVS ( hgvsString ):

    hgvs_split = hgvsString.split(':')
    print ( hgvs_split )
    return ( hgvs_split[0], hgvs_split[1] )

##------------------------------------------------------------------------------

standard_aa_names = [ "Ala", "Cys", "Asp", "Glu", "Phe", "Gly", "His", "Ile", "Lys",
                      "Leu", "Met", "Asn", "Pro", "Gln", "Arg", "Ser", "Thr", "Val",
                      "Trp", "Xaa", "Tyr", "Glx", "Ter" ]

aa1 = "ACDEFGHIKLMNPQRSTVWXYZ*"
aa3 = standard_aa_names
aa3_upper = [x.upper() for x in standard_aa_names]

def createTLAstring ( slaString ):

    print ( "     in createTLAstring ... ", slaString )

    s = slaString

    ii = 0
    done = False
    while not done:

        if ( s[ii] in aa1 ):
            ns = standard_aa_names[aa1.find(s[ii])]
            s = s[:ii] + ns + s[ii+1:]
            ii += 3
            print ( " --> s: ", s )
        else:
            ii += 1
            print ( " skipping this character " )

        if ( ii >= len(s) ): done = True

    return ( s )

##------------------------------------------------------------------------------

def parseAminoAcids ( aaString ):

    print ( " in parseAminoAcids ... ", aaString )

    try:
        aa_split = aaString.split('/')
        tla0 = createTLAstring ( aa_split[0] )
        tla1 = createTLAstring ( aa_split[1] )

        if ( aa_split[0] == '-' ):
            print ( tla0, tla1, 0, len(aa_split[1]) )
            return ( tla0, tla1, len(aa_split[0]), len(aa_split[1]) )
        else:
            print ( tla0, tla1, len(aa_split[0]), len(aa_split[1]) )
            return ( tla0, tla1, len(aa_split[0]), len(aa_split[1]) )

    except:
        return ( '', '', 0, 0 )

##------------------------------------------------------------------------------

def parseCodons ( codonString ):

    try:
        codon_split = codonString.split('/')
        print ( codon_split )

        ref = ''
        for b in codon_split[0]:
            if ( b in ['A','C','G','T'] ): ref += b
        var = ''
        for b in codon_split[1]:
            if ( b in ['A','C','G','T'] ): var += b

        print ( ref, var, len(ref), len(var) )
        return ( ref, var, len(ref), len(var) )

    except:
        return ( '', '', 0, 0 )

##------------------------------------------------------------------------------

def checkVEPinfo ( df ):

    j1 = df.to_json(orient='records',lines=True)
    rows = j1.splitlines(keepends=False)

    for r in rows:
        print ( "\n\n\n" )
        print ( r )
        try:
            d = json.loads(r)
        except:
            print ( " WHY DID THIS HAPPEN in checkVEPinfo ??? ", len(rows) )
            print ( r )
            sys.exit(-1)

        cAsterisk = 0
        cPlus = 0
        cMinus = 0
        cUnderscore = 0

        isFrameshiftVar = 0
        isStopLost = 0
        isInsertion = 0

        coding_change0 = ''
        coding_change1 = ''
        protein_change0 = ''
        protein_change1 = ''

        iStrand = 0

        newcString = ''
        newpString = ''

        if ( 'vep_input_hgvs' in d ): print ( d['vep_input_hgvs'] )
        ( gene_symbol, change_string ) = parseHGVS ( d['vep_input_hgvs'] )
        if ( change_string.startswith('c.') ): 
            coding_change0 = change_string
            if ( coding_change0.startswith('c.*') ): cAsterisk = 1
            if ( coding_change0.find('+') > 0 ): cPlus = 1
            if ( coding_change0.find('-') > 0 ): cMinus = 1
            if ( coding_change0.find('_') > 0 ): cUnderscore = 1

        elif ( change_string.startswith('p.') ):
            protein_change0 = change_string

        if ( 'coding_change' in d ):
            if ( d['coding_change'] is not None ):
                coding_change1 = d['coding_change'] 
                if ( coding_change0 != '' ):
                    if ( coding_change1 != coding_change0 ):
                        print ( " ERROR ??? inconsistent coding changes ??? !!! ", coding_change0, coding_change1 )

        if ( 'protein_change' in d ): 
            if ( d['protein_change'] is not None ):
                protein_change1 = d['protein_change'] 
                if ( protein_change0 != '' ):
                    if ( protein_change1 != protein_change0 ):
                        print ( " ERROR ??? inconsistent protein changes ??? !!! ", protein_change0, protein_change1 )

        if ( 'most_severe_consequence' in d ): 
            if ( d['most_severe_consequence'] is not None ):
                msq = d['most_severe_consequence'] 
                if ( msq == 'stop_lost' ): isStopLost = 1

        if ( 'strand' in d ):
            iStrand = int ( d['strand'] )
        else:
            print ( " how can we not know the strand !!! ??? " )
            print ( d )
            sys.exit(-1)

        print ( coding_change0, coding_change1, protein_change0, protein_change1, iStrand, msq )
        
        if ( 'codons' in d and d['codons'] is not None ):
            print ( " --> have codons : ", d['codons'] )
            print ( "     try to create the coding change string ??? " )
            ( ref, var, refLen, varLen ) = parseCodons ( d['codons'] )
            lenDiff = varLen - refLen
            if ( refLen != varLen ): print ( " NB: ref and var have different lengths !!! " )
            if ( lenDiff%3 != 0 ): 
                isFrameshiftVar = 1
                print ( " frameshift !!! " )

            ## example ... https://grch37.rest.ensembl.org/vep/human/hgvs/KRAS:c.34_35delGGinsTT?

            if ( not (cAsterisk+cPlus+cMinus+cUnderscore+isFrameshiftVar) ):
                try:
                    cds_start = int ( d['cds_start'] )
                    cds_end = int ( d['cds_end'] )
                    cds_diff = cds_end - cds_start
                    if ( cds_diff == 0 ):
                        print ( " don't know what to do with this yet ... ", cds_start, cds_end, ref, var )
                        newcString = "c." + str(cds_start) + ref + ">" + var
                    elif ( cds_diff == 1 ):
                        if ( refLen == (cds_diff + 1) ):
                            newcString = 'c.' + str(cds_start) + '_' + str(cds_end) + 'del' + ref
                            if ( varLen > 0 ):
                                newcString += 'ins' + var

                        else:
                            print ( " this should be the easy case ... " )
                            print ( cds_start, cds_end, cds_diff )
                    else:
                        print ( " don't know what to do with this yet ... " )

                    if ( newcString != '' ):
                        print ( " >==> newcString = <%s> " % newcString )
                        if ( coding_change0 != '' ):
                            if ( newcString == coding_change0 ): 
                                print ( " MATCH !!! (c0) ", newcString, coding_change0 )
                                if ( 'coding_change' not in d or d['coding_change'] is None ):
                                    print ( "     >==> setting coding_change ... using newly created string " )
                                    d['coding_change'] = newcString

                            else:
                                if ( 'coding_change' not in d or d['coding_change'] is None ):
                                    print ( "     >==> setting coding_change ... using vep_input_hgvs string " )
                                    d['coding_change'] = coding_change0

                        if ( coding_change1 != '' ):
                            if ( newcString == coding_change1 ): 
                                print ( " MATCH !!! (c1) ", newcString, coding_change1 )
                                print ( "     (but no need to set coding_change string ...) " )
                            else:
                                print ( " NOT a match ??? (c1) ", newcString, coding_change1 )
                                print ( " WHAT NOW ??? !!! " )

                except:
                    print ( "      try failed somewhere ... (a) " )
                    continue
            else:
                print ( " --> not trying to create coding change string ... " )

        if ( 'amino_acids' in d and d['amino_acids'] is not None ):
            print ( " --> have amino_acids : ", d['amino_acids'] )
            print ( "     try to create the protein change string ??? " )
            ( refAA, varAA, raaLen, vaaLen ) = parseAminoAcids ( d['amino_acids'] )

            if ( raaLen == 0 ): isInsertion = 1
            if ( vaaLen > raaLen ): isInsertion = 1

            if ( not (isStopLost+isInsertion+isFrameshiftVar) ):
                try:
                    protein_start = int ( d['protein_start'] )
                    protein_end = int ( d['protein_end'] )
                    prot_diff = protein_end - protein_start
                    if ( prot_diff == 0 ):
                        newpString = "p." + refAA + str(protein_start) + varAA
                    
                    if ( newpString != '' ):
                        print ( " >==> newpString = <%s> " % newpString )
                        if ( protein_change0 != '' ):
                            if ( newpString == protein_change0 ): 
                                print ( " MATCH !!! (p0) ", newpString, protein_change0 )
                                if ( 'protein_change' not in d or d['protein_change'] is None ):
                                    print ( "     >==> setting protein_change ... using newly created string " )
                                    d['protein_change'] = newcString

                            else:
                                if ( 'protein_change' not in d or d['protein_change'] is None ):
                                    print ( "     >==> setting protein_change ... using vep_input_hgvs string " )
                                    d['protein_change'] = protein_change0

                        if ( protein_change1 != '' ):
                            if ( newpString == protein_change1 ): 
                                print ( " MATCH !!! (p1) ", newpString, protein_change1 )
                                print ( "     (but no need to set protein_change string ...) " )
                            else:
                                print ( " NOT a match ??? (p1) ", newpString, protein_change1 )
                                print ( " WHAT NOW ??? !!! " )

                except:
                    print ( "      try failed somewhere ... (b) " )
                    continue
            else:
                print ( " --> not trying to create protein change string ... " )


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
    print ( " in vepFixup ... ", args )
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

    print ( " calling checkVEPinfo ... " )
    df2 = checkVEPinfo ( df )

    sys.exit(-1)

    print ( " calling myToJson ... " )
    myToJson ( fhOut, df )

##------------------------------------------------------------------------------

if __name__ == '__main__':

  t0 = time.time()

  parser = argparse.ArgumentParser()

  ## 
  parser.add_argument ( '-f',  '--inputVEPjsonFileName',  action='store', help='input JSON file', required=True, dest='inputFile', type=str )
  parser.add_argument ( '-o',  '--outputVEPjsonFileName', action='store', help='output JSON file', required=True, dest='outputFile', type=str )

  args = parser.parse_args()

  main ( args )

  t1 = time.time()

  print ( ' --> time taken in seconds: {dt}'.format(dt=(t1-t0)) )

##------------------------------------------------------------------------------
