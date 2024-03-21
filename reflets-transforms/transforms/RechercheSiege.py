# This Maltego Transformer is designed perform research in Pappers company database for address mentionned in company documents.
# Very usefull to explore HeadquartersAddress entities.

import copy
import sys
from transforms import papperparse
import re
import html

from extensions import registry
from maltego_trx.entities import Company
from maltego_trx.maltego import UIM_TYPES, MaltegoMsg, MaltegoTransform
from maltego_trx.transform import DiscoverableTransform


# Configure paparmeters to search Headquarters location in Pappers FR V2 API
def create_payload_siege(request) :
    config = papperparse.load_api_key_config()

    payload_tpl = {}
    payload_tpl['api_token'] = config['pappers']['api_key']
    q = "\"" + request.getProperty("streetaddress") + "\" " + request.getProperty("postalcode") + " " + re.sub(r'(?is)\d+\s*$', '', request.getProperty("city")  )
    payload_tpl['q'] = q

    payload_tpl['siege'] = "true"
    payload_tpl['precision'] = "exacte"
    payload_tpl['bases'] = "entreprises,dirigeants,beneficiaires,documents,publications" 

    return payload_tpl   

# Configure paparmeters to search Headquarters location in Pappers IN V1 API
def create_payload_siege_in(request) :
    config = papperparse.load_api_key_config()

    payload_tpl = {}
    payload_tpl['api_token'] = config['pappers']['api_key']
    q = "\"" + request.getProperty("streetaddress") + "\" " + request.getProperty("city") 
    payload_tpl['q'] = q

    payload_tpl['siege'] = "true"
    payload_tpl['precision'] = "exacte"
    payload_tpl['bases'] = "entreprises,dirigeants,beneficiaires,documents,publications" 

    return payload_tpl  


# Parse mentions from the address to configure link text. (Pappers V2 FR API)
# All mentions of the address will be added to the company's notes for furtehr investigation
def generate_siege_mention_link ( entity, entreprise ) :
    link_label = ""
    note = ""
    if 'documents' in entreprise and len(entreprise['documents']) > 0 :
        link_label = "Mention in :"
        note = "Mentionned in :\n\n"
        for document in  entreprise['documents'] : 
            if 'type' in document :
                #link_label += f"{document['type']} "
                note += f"Type: {document['type']}\n"
            if 'date_depot' in document :
                link_label += f"{document['date_depot']} "
                note += f"Type: {document['date_depot']}\n"
            if 'mentions' in document and len(document['mentions']) > 0 :
                for mention in document['mentions'] : 
                    m = mention.encode("ascii","ignore")
                    note += f"  {m}\n"
            
    note2 = html.escape(re.sub(r'(?is)<(?:/)?em>', '', note))  
    entity.setLinkLabel(link_label)
    entity.setNote(note2)
    entity.reverseLink()

# Parse mentions from the address to configure link text. (Pappers V1 IN API)
# All mentions of the address will be added to the company's notes for furtehr investigation
def generate_siege_mention_link_in ( entity, entreprise ) :
    link_label = ""
    note = ""
    if 'publications' in entreprise and len(entreprise['publications']) > 0 :
        link_label = "Mention in :"
        note = "Mentionned in :\n\n"
        for document in  entreprise['publications'] : 
            if 'type' in document :
                #link_label += f"{document['type']} "
                note += f"Type: {document['type']}\n"
            if 'date' in document :
                link_label += f"{document['date']} "
                note += f"Type: {document['date']}\n"
            if 'mentions' in document and len(document['mentions']) > 0 :
                for mention in document['mentions'] : 
                    m = mention.encode("ascii","ignore")
                    note += f"  {m}\n"
            
    note2 = html.escape(re.sub(r'(?is)<(?:/)?em>', '', note))   
    entity.setLinkLabel(link_label)
    entity.setNote(note2)
    entity.reverseLink()


@registry.register_transform(display_name="Pappers.fr - Recherche de siège", input_entity="maltego.Location",
                             description='Pappers.fr - Recherche de siège"',
                             output_entities=["reflets.DetailedCompany"])
class RechercheSiege(DiscoverableTransform):

    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):

        # Peppers API is strange when returning the number of page / result
        # Normally there is a "total" who defines the number of returning results. But it seems wrong (5 for Nicolas Sarkozy but more results)
        nbr_page = 100

        # This variable is used to limit the number of page browsed. At None, browss all the pages
        limit_page = 5
        current_page = 1
        
        try:
            # "countrysc" is the property containing COUNTRY_CODE in Location entities (origin of HeadquartersLocation)
            country_code = request.getProperty('countrysc')
            #sys.stderr.write(f"Country code: {country_code} {str(request.Properties)}") 

            if country_code is not None and ( country_code == 'CH' or country_code == 'UK' or country_code == 'GB' or country_code == 'BE') :
                payload_tpl = create_payload_siege_in(request)
                payload_tpl['par_page'] = '20'

                # XXX For compatibility with Maltego country code and flags
                if country_code == 'GB' : payload_tpl['country_code'] = 'UK'
                else : payload_tpl['country_code'] = country_code

                #sys.stderr.write(f"Recherche: {payload_tpl}")

                try:
                    current_page = 1

                    while current_page <= nbr_page:
                        payload = copy.deepcopy(payload_tpl)
                        payload['page'] = current_page

                        #sys.stderr.write(f"Response: {json.dumps(payload, indent=4)}") 
                        json_res = papperparse.make_request("https://api.pappers.in/v1/search", payload)
                        #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}") 

                        # Get the number of pages to browse
                        if limit_page is None : 
                            nbr_page = json_res['total']
                        else : nbr_page = limit_page
                        nbr_result = 0

                        for entreprise in json_res['results']:
                            nbr_result += 1

                            # Parse the companies that mentions the location in their documents
                            try : 
                                entity = papperparse.parse_entreprise_in( response , entreprise )
                                generate_siege_mention_link_in(entity, entreprise)          
                            except Exception as e :
                                sys.stderr.write(f"Error: {e}\n")
                                sys.stderr.write(f"Problem company parsing in Headquarters Location search for Pappers IN {country_code}\n")

                        # We set two conditions to stop querying the API
                        # We browse the number of pages and we stop if one call has return empty result
                        current_page += 1
                        if nbr_result == 0:
                            break

                except Exception as e:
                    response.addUIMessage(f"Error: {e}")                    

                return                 

            # No Pappers IN API country codes (UK, BE, CH) so searching in FR V2 API 

            payload_tpl = create_payload_siege(request)
            payload_tpl['par_page'] = '20'
            #sys.stderr.write(f"Recherche: {payload_tpl}")

            try:
                current_page = 1

                while current_page <= nbr_page:
                    payload = copy.deepcopy(payload_tpl)
                    payload['page'] = current_page

                    #sys.stderr.write(f"Response: {json.dumps(payload, indent=4)}") 
                    json_res = papperparse.make_request("https://api.pappers.fr/v2/recherche", payload)
                    #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}") 

                    # Get the number of pages to browse
                    if limit_page is None : 
                        nbr_page = json_res['total']
                    else : nbr_page = limit_page
                    nbr_result = 0

                    for entreprise in json_res['resultats']:
                        nbr_result += 1

                        # Parse the companies that mentions the location in their documents
                        try : 
                            entity = papperparse.parse_entreprise( response , entreprise )  
                            generate_siege_mention_link(entity, entreprise)          
                        except Exception as e :
                            sys.stderr.write(f"Error: {e}\n")
                            sys.stderr.write(f"Problem company parsing in Headquarters Location search for Pappers FR\n")

                    # We set two conditions to stop querying the API
                    # We browse the number of pages and we stop if one call has return empty result
                    current_page += 1
                    if nbr_result == 0:
                        break

            except Exception as e:
                response.addUIMessage(f"Error: {e}")


        except Exception as e:
            response.addUIMessage(f"Error: {e}\n")

