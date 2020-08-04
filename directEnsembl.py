



import json
import requests
import sys

from pprint import pprint
from requests.exceptions import HTTPError

## ----------------------------------------------------------------------------
def fetch_endpoint(server, request, content_type):

    try:

        print ( " *********************** " )
        print ( " endpoint request: ", server+request )
        r = requests.get(server+request, headers={"Content-Type":content_type})
        print ( r )
        print ( " *********************** " )

        r.raise_for_status()

    except HTTPError as http_err:
        print ( 'HTTP error occurred: {http_err}' )

    except Exception as err:
        print ( 'Other error occurred: {err}' )

    else:
        print ( 'successful request!')

    if content_type == 'application/json':
        return r.json()
    else:
        return r.text

## ----------------------------------------------------------------------------

## this is one of their published examples, which I don't really understand
if ( 0 ):
    server = "http://rest.ensembl.org/"
    ext = "lookup/id/TraesCS3D02G007500?"
    con = "application/json"
    r = fetch_endpoint(server, ext, con)
    pprint ( r )

## how about BRAF?
if ( 0 ):
    server = "http://rest.ensembl.org/"
    ext = "lookup/symbol/homo_sapiens/BRAF?"
    con = "application/json"
    r = fetch_endpoint(server, ext, con)
    pprint ( r )

## here is a variant call in torrent_results:
##       _locus                             chr7:140494203
##       chrom                              chr7                'chr'+r['seq_region_name']
##       position                           140494203
##       snpeff_annotated_dna_change        c.1045G>T
##       snpeff_annotated_gene              BRAF                r['display_name']
##       snpeff_annotated_protein_change    p.Ala349Ser
##       snpeff_annotated_transcript        NM_004333.4         r['id']=ENSG00000157764 r['version']=8
##       ref                                C
##       unique_mut_identifier              chr7:140494203:A
##                                                              r['strand']=-1


## how about BRAF on GRCh37?
if ( 0 ):
    server = "http://grch37.rest.ensembl.org/"
    ext = "lookup/symbol/homo_sapiens/BRAF?"
    con = "application/json"
    r = fetch_endpoint(server, ext, con)
    pprint ( r )

if ( 1 ):
    server = "http://grch37.rest.ensembl.org/"
    ext = "vep/homo_sapiens/hgvs/BRAF:c.1045G>T?"
    con = "application/json"
    r = fetch_endpoint(server, ext, con)
    pprint ( r )

if ( 0 ):
    ## this works for the GRCh38 endpoint ...
    server = "http://rest.ensembl.org/"
    ext = "variant_recoder/human/BRAF:c.1045G>T?"
    con = "application/json"
    r = fetch_endpoint(server, ext, con)
    pprint ( r )



