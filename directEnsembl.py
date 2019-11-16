



import requests, sys, json
from pprint import pprint

def fetch_endpoint(server, request, content_type):

    r = requests.get(server+request, headers={ "Content-Type" : content_type})

    if not r.ok:
        r.raise_for_status()
        sys.exit()

    if content_type == 'application/json':
        return r.json()
    else:
        return r.text


server = "http://rest.ensembl.org/"
ext = "lookup/id/TraesCS3D02G007500?"
con = "application/json"
getr = fetch_endpoint(server, ext, con)
pprint ( getr )


server = "http://grch37.rest.ensembl.org/"
ext = "variant_recoder/human/rs116035550?"
getr = fetch_endpoint(server, ext, con)
pprint ( getr )


ext = "variant_recoder/human/NC_000011.9:212463:G:A?"
ext = "variant_recoder/human/NC_000007.13:101837173:C:T?"
getr = fetch_endpoint(server, ext, con)
pprint ( getr )

