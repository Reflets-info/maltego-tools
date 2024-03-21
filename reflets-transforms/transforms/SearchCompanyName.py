# This Maltego Transformer is designed perform research in Pappers company database by name of the company
# Very usefull when we have a company mentionned without it's number (siret/vat)

import copy
import json
import sys

from transforms import papperparse

from extensions import registry
from maltego_trx.entities import Company
from maltego_trx.maltego import UIM_TYPES, MaltegoMsg, MaltegoTransform
from maltego_trx.transform import DiscoverableTransform


@registry.register_transform(display_name="Pappers.fr - Search Company buy name", input_entity="maltego.Person",
                             description='Pappers.fr - Search Officer',
                             output_entities=["maltego.Company"])
class SearchCompanyName(DiscoverableTransform):

    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):

        # Pappers API is strange when returning the number of page / result
        # Normally there is a "total" who defines the number of returning results. But it seems wrong (5 for Nicolas Sarkozy but more results)
        nbr_page = 100

        # This variable is used to limit the number of page browsed. At None, browss all the pages
        limit_page = 5
        current_page = 1

        # If we have a country_code that is supported by Pappers IN V1 API, we use it
        country_code = request.getProperty('countrycode')
        if country_code is not None and ( country_code == 'CH' or country_code == 'UK' or country_code == 'GB' or country_code == 'BE' or country_code == 'FR') :

            payload_tpl = papperparse.create_payload_company(request)
            payload_tpl['par_page'] = '20'

            # XXX For compatibility with Maltego country code and flags
            if country_code == 'GB' : payload_tpl['country_code'] = 'UK'
            else : payload_tpl['country_code'] = country_code

            #sys.stderr.write(f"Recherche: {payload}")
            try:
                current_page = 1

                while current_page <= nbr_page:
                    payload = copy.deepcopy(payload_tpl)
                    payload['page'] = current_page

                    json_res = papperparse.make_request("https://api.pappers.in/v1/search", payload)
                    sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}")

                    # Get the number of pages to browse
                    if limit_page is None : 
                        nbr_page = json_res['total']
                    else :
                        nbr_page = limit_page
                    nbr_result = 0

                    # Parse resulting company from the serach call
                    for company in json_res['results']:
                        nbr_result += 1
                        
                        try :
                            entity = papperparse.parse_company( response , company, country_code )
                        except Exception as e :
                            sys.stderr.write(f"Error: {e}\n")
                            sys.stderr.write(f"Entreprise: {json.dumps(company, indent=4)}\n")
                                    

                    # We set two conditions to stop querying the API
                    # We browse the number of pages and we stop if one call has return empty result
                    current_page += 1
                    if nbr_result == 0:
                        break

            except Exception as e:
                response.addUIMessage(f"Error: {e}")
    
