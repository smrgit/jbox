

import ensembl_rest

r = ensembl_rest.symbol_lookup ( species='homo sapiens', symbol='BRCA2' )

print ( r )

r = ensembl_rest.variant_recoder ( species='homo sapiens', id='NC_000007.13:101837173:C:T' )

print ( r )


