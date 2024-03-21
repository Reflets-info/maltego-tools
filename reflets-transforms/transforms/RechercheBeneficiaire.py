# This Maltego Transformer is designed perform search of a BENEFICIARY in the Pappers FR V2 API

import copy
import json
import sys

from transforms import papperparse

import requests

from extensions import registry
from maltego_trx.entities import Company
from maltego_trx.maltego import UIM_TYPES, MaltegoMsg, MaltegoTransform
from maltego_trx.transform import DiscoverableTransform


@registry.register_transform(display_name="Pappers.fr - Recherche bénéficiaires", input_entity="maltego.Person",
                             description='Pappers.fr - Recherche bénéficiaires',
                             output_entities=["maltego.Company"])
class RechercheBeneficiaire(DiscoverableTransform):

    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):

        # Peppers API is strange when returning the number of page / result
        # Normally there is a "total" who defines the number of returning results. But it seems wrong (5 for Nicolas Sarkozy but more results)
        nbr_page = 100

        # This variable is used to limit the number of page browsed. At None, browss all the pages
        limit_page = 5
        current_page = 1
        
        # BENEFICIAIRE SEARCH TERMS 
        # Using most precise key info to get the good guy
        payload_tpl = papperparse.create_payload_beneficiaries( request )
        payload_tpl['par_page'] = '20'
        #sys.stderr.write(f"Recherche: {payload_tpl}")

        try:
            current_page = 1

            while current_page <= nbr_page:
                payload = copy.deepcopy(payload_tpl)
                payload['page'] = current_page

                json_res = papperparse.make_request("https://api.pappers.fr/v2/recherche-beneficiaires", payload)
                sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}")

                # Get the number of pages to browse
                if limit_page is None : 
                    nbr_page = json_res['total']
                else :
                    nbr_page = limit_page
                nbr_result = 0

                for dirigeant in json_res['resultats']:
                    nbr_result += 1

                    # Filter to remove noise from search result
                    if ( papperparse.do_filter_dirigeant_entity( request , dirigeant ) ) :
                        continue

                    for entreprise in dirigeant['entreprises']:
                        try :

                            entity = papperparse.parse_entreprise( response , entreprise)
                            papperparse.generate_beneficiaire_link_config(  entity, entreprise['beneficiaire'] )
                            entity.reverseLink()
                        
                        except Exception as e :
                            sys.stderr.write(f"Error: {e}\n")
                            sys.stderr.write(f"Entreprise: {json.dumps(entreprise, indent=4)}\n")

                    # Auto-qualification of the calling entity to be able to update it.
                    try : 
                        entity = papperparse.auto_parse_dirigeant( request, response , dirigeant )            
                    except Exception as e :
                        sys.stderr.write(f"Error: {e}\n")
                        sys.stderr.write(f"Problem in the main result parsing for auto-qualification\n")

                    # We don't do this anymore to prevent dplicated links with RechercheDirigeant Transform    
                    '''        
                    for entreprise in dirigeant['entreprises_dirigeant']:
                        try :

                            entity = papperparse.parse_entreprise( response , entreprise)
                            papperparse.create_dirigeant_link_config(  entity, entreprise )

                        except Exception as e :
                            sys.stderr.write(f"Error: {e}\n")
                            sys.stderr.write(f"Entreprises_dirigeant: {json.dumps(entreprise, indent=4)}\n")                          
                    
                    '''

                # We set two conditions to stop querying the API
                # We browse the number of pages and we stop if one call has return empty result
                current_page += 1
                if nbr_result == 0:
                    break

        except Exception as e:
            response.addUIMessage(f"Error: {e}")    
        

