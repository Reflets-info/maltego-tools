import copy
import json
import sys
import yaml

import requests

from extensions import registry
from maltego_trx.entities import Company
from maltego_trx.maltego import UIM_TYPES, MaltegoMsg, MaltegoTransform
from maltego_trx.transform import DiscoverableTransform


# Apply filters on search result to get only pertinent results
def do_filter_entity( request , dirigeant ) :
    #sys.stderr.write(f"Filter: {dirigeant['nom']} {request.getProperty('lastname')}")
    # Filter to remove noise from search result
    if (
        dirigeant['prenom_usuel'] != request.getProperty('firstname') 
        or (dirigeant['nom']).casefold().replace("-"," ") != request.getProperty('lastname').casefold().replace("-"," ")
        ) :
        return 1
    if request.getProperty('date_naissance') is not None and request.getProperty('date_naissance') != dirigeant['date_de_naissance_formate'] :
        return 1
    if request.getProperty('age') is not None and int(request.getProperty('age')) != dirigeant['age'] :
        return 1    


# Extract from Pappers JSON the details of an entreprise and map it to specific Maltego entities
def parse_details_entreprise( entity , entreprise ):

        entity.addProperty("id_tax_number","siren_vat","strict",entreprise['siren'])
        entity.addProperty("nom_usuel","Nom","loose",entreprise['nom_entreprise'])
        entity.addProperty("headquarters_address","Siege","loose",f"{entreprise['siege']['adresse_ligne_1']}")
        entity.addProperty("headquarters_city","Siege","loose",f"{entreprise['siege']['ville']}")
        entity.addProperty("date_creation","Creation date","loose",f"{entreprise['date_creation']}")
        entity.addProperty("activity","Activity","loose",f"{entreprise['libelle_code_naf']}")
        entity.addProperty("forme_juridique","Forme juridique","loose",f"{entreprise['forme_juridique']}")

        if 'date_cessation' in entreprise and entreprise['date_cessation'] is not None :
            entity.addProperty("date_cessation","Date cessation","loose",f"{entreprise['date_cessation']}")

        return entity      


# Extract information related to beneficiaries and set the link with the correct message
def create_beneficiaire_link_config ( entity , entreprise ) :
    # Create the LINK LABEL : Relation du dirigeant à l'entreprise crée
    link_label = f"Parts: {entreprise['beneficiaire']['pourcentage_parts']} / Votes: {entreprise['beneficiaire']['pourcentage_votes']}"
    entity.setLinkLabel(link_label)
    entity.setLinkColor( "#946b2d" )


# Extract information related to the dirigeant and set the link with the correct message
def create_dirigeant_link_config ( entity , entreprise ) :
    if entreprise['dirigeant'] is not None:
        
        # Create the LINK LABEL : Relation du dirigeant à l'entreprise crée
        link_label = f"{entreprise['dirigeant']['qualites'][0]}"

        #sys.stderr.write(f"{qualite_dirigeant} actuel depuis {entreprise['dirigeant']['date_prise_de_poste']}")
        if entreprise['dirigeant']['actuel'] is True :
            link_label = f"{entreprise['dirigeant']['qualites'][0]} actuel depuis {entreprise['dirigeant']['date_prise_de_poste']}"
        else :
            link_label = f"Ancien {entreprise['dirigeant']['qualites'][0]} arrivé {entreprise['dirigeant']['date_prise_de_poste']}"
            # Style : Dashed for old positions
            entity.setLinkStyle(1)

    entity.setLinkLabel(link_label)
    entity.setLinkColor( "#657a8b" )



@registry.register_transform(display_name="Dirigeant To Company", input_entity="maltego.Person",
                             description='Receive Person and use Pappers API to get associated Compagnies.',
                             output_entities=["maltego.Company"])
class PersonPappers(DiscoverableTransform):


    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):

        # Peppers API is strange when returning the number of page / result
        # Normally there is a "total" who defines the number of returning results. But it seems wrong (5 for Nicolas Sarkozy but more results)
        nbr_page = 100

        # This variable is used to limit the number of page browsed. At None, browss all the pages
        limit_page = 5
        current_page = 1
        payload_tpl = {}

        # DIRIGEANT SEARCH TERMS 
        # Using most precise key info to get the good guy
        with open('./transforms/api_keys.yml', 'r') as file :
            config = yaml.safe_load(file)
            
        payload_tpl['api_token'] = config['pappers']['api_key']
        payload_tpl['par_page'] = '20'

        # If we know several "prenoms", we search with them
        if request.getProperty('prenoms') is not None :        
            payload_tpl['q'] = request.getProperty('prenoms') + " " + request.getProperty('lastname')
        else :
            payload_tpl['q'] = request.getProperty('firstname') + " " + request.getProperty('lastname')  

        # Create a birth date filter based on the RGPD compliant birth date printed on the site 
        if request.getProperty('date_naissance') is not None : 
            payload_tpl['date_de_naissance_dirigeant_min'] = (request.getProperty('date_naissance')).replace('/','-')
            payload_tpl['date_de_naissance_dirigeant_max'] = (request.getProperty('date_naissance')).replace('/','-')

        # Create a age filter : thisnone seems to work
        if request.getProperty('age') is not None : 
            payload_tpl['age_dirigeant_min'] = request.getProperty('age')
            payload_tpl['age_dirigeant_max'] = request.getProperty('age')

        #sys.stderr.write(f"Recherche: {payload_tpl}")

        try:
            current_page = 1

            while current_page <= nbr_page:
                payload = copy.deepcopy(payload_tpl)
                payload['page'] = current_page

                page = requests.get("https://api.pappers.fr/v2/recherche-dirigeants", params=payload)
                if page.status_code == 401:
                    raise Exception("Bad API key")
                elif page.status_code == 404:
                    raise Exception("No results !")
                elif page.status_code == 503:
                    raise Exception("Service unavailable : try again later")
                elif page.status_code == 200:
                    json_res = page.json()

                    #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}")

                    # Get the number of pages to browse
                    if limit_page is None : 
                        nbr_page = json_res['total']
                    else :
                        nbr_page = limit_page

                    nbr_result = 0
                    for dirigeant in json_res['resultats']:
                        nbr_result += 1

                        # Filter to remove noise from search result
                        if ( do_filter_entity( request , dirigeant ) ) :
                            continue

                        for entreprise in dirigeant['entreprises']:
                            entity = response.addEntity("reflets.DetailedCompany", entreprise['siren'] )
                            create_dirigeant_link_config(  entity, entreprise )
                            parse_details_entreprise( entity, entreprise) 

                    # We set two conditions to stop querying the API
                    # We browse the number of pages and we stop if one call has return empty result
                    current_page += 1
                    if nbr_result == 0:
                        break


                else:
                    raise Exception("Unknown error code !")

        except Exception as e:
            response.addUIMessage(f"Error: {e}")
        
        
        # BENEFICIAIRE SEARCH TERMS 
        # Using most precise key info to get the good guy
        payload_tpl = {}
        payload_tpl['api_token'] = config['pappers']['api_key']
        payload_tpl['par_page'] = '20'

        # If we know several "prenoms", we search with them
        if request.getProperty('prenoms') is not None :        
            payload_tpl['q'] = request.getProperty('prenoms') + " " + request.getProperty('lastname')
        else :
            payload_tpl['q'] = request.getProperty('firstname') + " " + request.getProperty('lastname')

        # Create a birth date filter based on the RGPD compliant birth date printed on the site 
        if request.getProperty('date_naissance') is not None : 
            payload_tpl['date_de_naissance_beneficiaire_min'] = (request.getProperty('date_naissance')).replace('/','-')
            payload_tpl['date_de_naissance_beneficiaire_max'] = (request.getProperty('date_naissance')).replace('/','-')

        # Create a age filter : thisnone seems to work
        if request.getProperty('age') is not None : 
            payload_tpl['age_beneficiaire_min'] = request.getProperty('age')
            payload_tpl['age_beneficiaire_max'] = request.getProperty('age')

        #sys.stderr.write(f"Recherche: {payload_tpl}")

        try:
            current_page = 1

            while current_page <= nbr_page:
                payload = copy.deepcopy(payload_tpl)
                payload['page'] = current_page

                page = requests.get("https://api.pappers.fr/v2/recherche-beneficiaires", params=payload)
                if page.status_code == 401:
                    raise Exception("Bad API key")
                elif page.status_code == 404:
                    raise Exception("No results !")
                elif page.status_code == 503:
                    raise Exception("Service unavailable : try again later")
                elif page.status_code == 200:
                    json_res = page.json()

                    #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}")

                    # Get the number of pages to browse
                    if limit_page is None : 
                        nbr_page = json_res['total']
                    else :
                        nbr_page = limit_page
                    nbr_result = 0

                    for dirigeant in json_res['resultats']:
                        nbr_result += 1

                        # Filter to remove noise from search result
                        if ( do_filter_entity( request , dirigeant ) ) :
                            continue

                        for entreprise in dirigeant['entreprises']:

                            entity = response.addEntity("reflets.DetailedCompany", entreprise['siren'] )
                            create_beneficiaire_link_config(  entity, entreprise )
                            parse_details_entreprise( entity, entreprise)

                        for entreprise in dirigeant['entreprises_dirigeant']:

                            entity = response.addEntity("reflets.DetailedCompany", entreprise['siren'] )
                            create_dirigeant_link_config(  entity, entreprise )
                            parse_details_entreprise( entity, entreprise)                            
 

                    # We set two conditions to stop querying the API
                    # We browse the number of pages and we stop if one call has return empty result
                    current_page += 1
                    if nbr_result == 0:
                        break

                else:
                    raise Exception("Unknown error code !")
        
        except Exception as e:
            response.addUIMessage(f"Error: {e}")    
        
        # Write the slider value as a UI message - just for fun
        response.addUIMessage(f"Slider value is at: {request.Slider}")

