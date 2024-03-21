# The PAPPERS APIs parsing library

import string
import sys
import yaml
import re
import requests

# PAPPERS API QUERYING

# Handling of API error code
def make_request ( url , payload ) : 

    page = requests.get( url , params=payload)
    if page.status_code == 401:
        raise Exception("Bad API key")
    elif page.status_code == 404:
        raise Exception("No results !")
    elif page.status_code == 503:
        raise Exception("Service unavailable : try again later")
    elif page.status_code == 200:
        json_res = page.json()
        return json_res
    else:
        raise Exception("Unknown error code !")


def load_api_key_config () :
    with open('./transforms/api_keys.yml', 'r') as file :
        config = yaml.safe_load(file)
    return config


## ENTREPRISE SEARCH TERMS (Pappers FR V2 API)
def create_payload_entreprise( request ) :
    config = load_api_key_config()

    payload_tpl = {}
    payload_tpl['api_token'] = config['pappers']['api_key']
    payload_tpl['siren'] = request.getProperty("id_tax_number")

    return payload_tpl

## COMAPNY SEARCH TERMS (Pappers IN V1 API) 
def create_payload_company( request ) :
    config = load_api_key_config()

    payload_tpl = {}
    payload_tpl['api_token'] = config['pappers']['api_key']
    payload_tpl['q'] = request.getProperty("nom_usuel")

    return payload_tpl

## BENEFICIAIRE SEARCH TERMS (Pappers FR V2 API)
## Using most precise key info to get the good guy
def create_payload_beneficiaries( request ) :
    config = load_api_key_config()

    payload_tpl = {}
    payload_tpl['api_token'] = config['pappers']['api_key']

    # If we know several "prenoms", we search with them
    if request.getProperty('prenoms') is not None :        
        payload_tpl['q'] = request.getProperty('prenoms') + " " + request.getProperty('lastname')
    else :
        payload_tpl['q'] = request.getProperty('firstname') + " " + request.getProperty('lastname')

    # Create a birth date filter based on the RGPD compliant birth date printed on the site 
    if request.getProperty('date_naissance') is not None : 
        filter_split = request.getProperty('date_naissance').split("-")
        filter = filter_split[2] + "-" + filter_split[1] + "-" + filter_split[0]
        payload_tpl['date_de_naissance_dirigeant_min'] = filter
        payload_tpl['date_de_naissance_dirigeant_max'] = filter

    # Create a age filter : thisnone seems to work
    if request.getProperty('age') is not None : 
        payload_tpl['age_beneficiaire_min'] = request.getProperty('age')
        payload_tpl['age_beneficiaire_max'] = request.getProperty('age')

    return payload_tpl


## DIRIGEANT SEARCH TERMS (Pappers FR V2 API)
## Using most precise key info to get the good guy
def create_payload_dirigeants( request ) :
    config = load_api_key_config()

    payload_tpl = {}
    payload_tpl['api_token'] = config['pappers']['api_key']

    # If we know several "prenoms", we search with them
    if request.getProperty('prenoms') is not None :        
        payload_tpl['q'] = request.getProperty('prenoms') + " " + request.getProperty('lastname')
    else :
        payload_tpl['q'] = request.getProperty('firstname') + " " + request.getProperty('lastname')  

    # Create a birth date filter based on the RGPD compliant birth date printed on the site 
    if request.getProperty('date_naissance') is not None : 
        filter_split = request.getProperty('date_naissance').split("-")
        filter = filter_split[2] + "-" + filter_split[1] + "-" + filter_split[0]
        payload_tpl['date_de_naissance_dirigeant_min'] = filter
        payload_tpl['date_de_naissance_dirigeant_max'] = filter

    # Create a age filter : thisnone seems to work
    if request.getProperty('age') is not None : 
        payload_tpl['age_dirigeant_min'] = request.getProperty('age')
        payload_tpl['age_dirigeant_max'] = request.getProperty('age')

    return payload_tpl




# PAPPERS JSON RESULT PARSING AND MALTEGO ENTITY CREATION


## Commons fonction designed to create accurate keys to optimize Maltego graph merging and API pappers searching

def normalize_siren ( siren ) :
    return siren.replace(" ","").replace(".", "")

def normalize_name (name ) :
    return string.capwords(name.replace("-"," "))

# XXX This is a hack 
# Sometimes "normalized month dates" comes differently from the API : 1972-05 1972-5. So we normalize to allow merging 
def normalize_date_month (name ) :
    return re.sub(r'(?is)\-0', '-', name)


# This is e generic funtion to calculate the KEY properties for Dirigeant Entities
# Calculate from a calculated IDENTITY object
def calculate_key ( identity ) :

    # Key construction, used for an accurate entity merging
    identity['key'] = identity['firstname'] + " " + identity['lastname']
    #if identity['birthdate'] is not None :
    #    identity['key'] += " " + identity['birthdate']
    if identity['birthdate_month'] is not None :
        identity['key'] += " " + identity['birthdate_month'] 


# Will put the little flag : 
# - compatibility between pappers and maltego
# - Mapping fuzziness of Pappers API
def parse_identity ( dirigeant ) :
    identity = {}
    identity['firstname'] = None
    identity['firstnames'] = None    
    identity['lastname'] = None
    identity['birthdate'] = None
    identity['birthdate_month'] = None
    identity['age'] = None    

    # XXX Handling differents parameter names in the Pappers API
    if 'date_de_naissance_formatee' in dirigeant and dirigeant['date_de_naissance_formatee'] is not None : 
        date_split = dirigeant['date_de_naissance_formatee'].split('/') 
        identity['birthdate_month'] = normalize_date_month(date_split[1] + "-" + date_split[0])
    elif 'date_de_naissance_rgpd' in dirigeant and dirigeant['date_de_naissance_rgpd'] is not None :  
        identity['birthdate_month'] = normalize_date_month(dirigeant['date_de_naissance_rgpd'])
    elif 'date_of_birth' in dirigeant and dirigeant['date_of_birth'] is not None : 
        identity['birthdate_month'] = normalize_date_month(dirigeant['date_of_birth'])

    if 'date_de_naissance_complete_formatee' in dirigeant and dirigeant['date_de_naissance_complete_formatee'] is not None : 
        date_split = dirigeant['date_de_naissance_complete_formatee'].split('/') 
        identity['birthdate'] = date_split[2] + "-" + date_split[1] + "-" + date_split[0] 
        if identity['birthdate_month'] is None : 
            identity['birthdate_month'] = normalize_date_month(date_split[2] + "-" + date_split[1])                
    elif 'date_de_naissance' in dirigeant and dirigeant['date_de_naissance'] is not None :  
        identity['birthdate'] = dirigeant['date_de_naissance'] 

    if 'prenom' in dirigeant and dirigeant['prenom'] is not None : 
        identity['firstnames'] = normalize_name(dirigeant['prenom'])

    if 'prenom_usuel' in dirigeant and dirigeant['prenom_usuel'] is not None : 
        identity['firstname'] = normalize_name(dirigeant['prenom_usuel'])
    elif 'first_name' in dirigeant and dirigeant['first_name'] is not None :

        # V1 IN API return several firstname in "firstname"
        name_split = dirigeant['first_name'].split(" ")
        identity['firstname'] = normalize_name(name_split[0]) 
        if identity['firstname'] is None :
            identity['firstnames'] = normalize_name(dirigeant['first_name'])           

    if 'nom' in dirigeant and dirigeant['nom'] is not None : 
        identity['lastname'] = normalize_name(dirigeant['nom'])
    elif 'last_name' in dirigeant and dirigeant['last_name'] is not None : 
        identity['lastname'] = normalize_name(dirigeant['last_name'])         

    if 'age' in dirigeant and dirigeant['age'] is not None : 
        identity['age'] = dirigeant['age']

    calculate_key(identity)

    return identity




# Will put the little flag : 
# - compatibility between pappers and maltego
# - Mapping fuzziness of Pappers API
def parse_location ( entity, siege ) :
    location = {}
    location['country_code'] = None
    location['country'] = None
    location['address'] = None
    location['city'] = None
    location['code_postal'] = None    
    ccode_found = False

    # COUNTRY_CODE : API Pappers FR V2
    if 'code_pays' in siege and siege['code_pays'] is not None : 
        location['country_code'] = siege['code_pays']
    # API Pappers IN V1
    elif 'country_code' in siege and siege['country_code'] is not None :
        location['country_code'] = siege['country_code']


    # COUNTRY : API Pappers FR V2
    if 'pays' in siege and siege['pays'] is not None :
        location['country'] = siege['pays']
    # API Pappers IN V1        
    elif 'country' in siege and siege['country'] is not None :
        location['country'] = siege['country']


    # ADDRESS : API Pappers FR V2
    if 'adresse_ligne_1' in siege and siege['adresse_ligne_1'] is not None :
        location['address'] = siege['adresse_ligne_1']
    # API Pappers IN V1        
    elif 'address_line_1' in siege and siege['address_line_1'] is not None :
        location['address'] = siege['address_line_1']
    # ADDRESS : API Pappers FR V2
    if 'adresse_ligne_2' in siege and siege['adresse_ligne_2'] is not None :
        location['address'] += ", " + siege['adresse_ligne_2']
    # API Pappers IN V1        
    elif 'address_line_2' in siege and siege['address_line_2'] is not None :
        location['address'] += ", " + siege['address_line_2']


    # CITY : API Pappers FR V2
    if 'ville' in siege and siege['ville'] is not None :
        location['city'] = siege['ville']
    # API Pappers IN V1        
    elif 'city' in siege and siege['city'] is not None :
        location['city'] = siege['city']

    # POSTAL CODE : API Pappers FR V2
    if 'code_postal' in siege and siege['code_postal'] is not None :
        #sys.stderr.write(f"Postal code {siege['code_postal']} \n")
        location['code_postal'] = siege['code_postal']
    # API Pappers IN V1        
    elif 'postal_code' in siege and siege['postal_code'] is not None :
        location['code_postal'] = siege['postal_code']        


    if location['country_code'] is not None : 
 
        # XXX compatibility with maltego flags
        if location['country_code'] == 'UK' :
            location['country_code'] = 'GB'
 
        ccode_found = True 

    elif location['country']  is not None : 
        if ccode_found is not True :
            ccode_found = True
            if location['country'] == 'France' :
                location['country_code'] = 'FR'  
            elif location['country'] == 'ETATS-UNIS' :
                location['country_code'] = 'US'
            elif location['country'] == 'United States' :
                location['country_code'] = 'US'
            elif location['country'] == 'England' :
                location['country_code'] = 'GB' 
            elif location['country'] == 'Switzerland' :
                location['country_code'] = 'CH'  
            elif location['country'] == 'Belgique' :
                location['country_code'] = 'BE'                                                                  
            elif location['country'] == 'LU' :
                location['country_code'] = 'LU'
            if location['country'] == 'Netherlands' :
                location['country_code'] = 'NL'
            if location['country'] == 'Luxembourg' :
                location['country_code'] = 'LU'                                                 
            else :
                ccode_found = False
                sys.stderr.write(f"Cannot find country code. Maybe add {location['country']} to the mapping\n")

    elif location['city']  is not None : 
        if ccode_found is not True :
            ccode_found = True
            if location['city'] == 'Zürich' :
                location['country_code'] = 'CH'                
            else :
                ccode_found = False
                sys.stderr.write(f"Cannot find country code. Maybe add {location['city']} to the mapping\n")

    return location
 


# Apply filters on search result to get only pertinent results
def do_filter_dirigeant_entity( request , dirigeant ) :

    # Identity calculation
    identity = parse_identity( dirigeant )

    try :
        if ( ( identity['firstname'] != request.getProperty('firstname') )or ( identity['lastname'] != request.getProperty('lastname') ) ) :
            sys.stderr.write(f"Dirigeant filtered : Names does'nt match : {identity['firstname']} != {request.getProperty('firstname')} or {identity['lastname']} != {request.getProperty('lastname')}\n")
            return 1

        # If we have birth_month in the calling entity and the results, we first check that      
        if (request.getProperty('date_naissance_rgpd') is not None ) and identity['birthdate_month'] is not None : 
            if ( request.getProperty('date_naissance_rgpd') != identity['birthdate_month'] ) :
                sys.stderr.write(f"Dirigeant filtered : Month Naissance does'nt match : {request.getProperty('date_naissance_rgpd')} != {identity['birthdate_month']} \n")
                return 1
            elif (request.getProperty('date_naissance') is not None ) and identity['birthdate'] is not None : 
                if ( request.getProperty('date_naissance') != identity['birthdate'] ) :
                    sys.stderr.write(f"Strange !! Dirigeant birth month match but not year (not filtered, LAZY) {request.getProperty('date_naissance')} != {identity['birthdate']} \n")

        # If we have matched month, we keep the entity
        # If not, we try to match on pirth date  
        elif (request.getProperty('date_naissance') is not None ) and identity['birthdate'] is not None : 
                if ( request.getProperty('date_naissance') != identity['birthdate'] ) :
                    sys.stderr.write(f"Dirigeant filtered : Date naissance doesn't match {request.getProperty('date_naissance')} != {identity['birthdate']} | {request.getProperty('date_naissance_rgpd')} != {identity['birthdate_month']}\n")
                    return 1

        elif request.getProperty('age') is not None and identity['age'] is not None :
            if int(request.getProperty('age')) != identity['age'] :
                sys.stderr.write(f"Dirigeant filtered : Age does not match : {request.getProperty('age')} != {identity['age']}\n")
                return 1 
        else :
            #Default behaviour for date / month / age filtering : LAZY and let pass
            return 0
        
    except Exception as e : 
            sys.stderr.write(f"Error while applying filters {e}\n") 
    return 0 








## ENTITY parsing - FR

### Parse Etablissement
def parse_etablissement( response , etablissement ) :
    entity = response.addEntity("reflets.HeadquartersLocation", "test")

    ### Location entity calculation
    location = parse_location(entity, etablissement )
    if location['address'] is not None : entity.addProperty("streetaddress","Street Address","strict",f"{location['address']}")
    if location['city'] is not None : entity.addProperty("city","City","loose",f"{location['city']}")
    if location['country'] is not None : entity.addProperty("country","Country","loose",f"{location['country']}")
    if location['code_postal'] is not None : entity.addProperty("postalcode","Postal code","loose",f"{location['code_postal']}")          
    if location['country_code'] is None :
        sys.stderr.write(f"Cannot find country code.\n")
        entity.addProperty("countrycode","Contry code","loose",'FR')
    else :   
        entity.addProperty("countrycode","Contry code","loose",location['country_code'])          

    entity.addProperty("activity","Activity","loose",etablissement['libelle_code_naf'])
    return entity

### Generate correct link configuration for HEADQUARTERS
def generate_siege_link_config( entity , etablissement ) :
    link_label = f"Siège since {etablissement['date_de_creation']}"
    entity.setLinkLabel(link_label)
    entity.setLinkThickness( 4 )

### Generate correct link configuration for ETABLISSEMENT
def generate_etablissement_link_config( entity , etablissement ) :
    link_label = ""
    if etablissement['date_cessation'] is not None :
        link_label = f"From {etablissement['date_de_creation']} to {etablissement['date_cessation']}"
    else :
        link_label = f"Since {etablissement['date_de_creation']}"
    entity.setLinkLabel(link_label)


### Parse Representant from Pappers JSON and map it to Dirigeant Custom Maltego Entity
def auto_parse_dirigeant( request, response , beneficiaire ) :

    # Location calculation
    identity = parse_identity(beneficiaire )

    sys.stderr.write(f"Tricks : " + str(request.getProperty("date_naissance")) + " " + str(request.getProperty("date_naissance_rgpd")) )
    # If we have no birthdate detected, but our calling entity as one.
    # And if birthdate_month are identical
    # Entity seems identical so we get the calling entity birthdate to allow merging
    if identity['birthdate'] is None and request.getProperty("date_naissance") != "" :

        if identity['birthdate_month'] is not None and identity['birthdate_month'] == request.getProperty("date_naissance_rgpd") :
            identity['birthdate'] = request.getProperty("date_naissance")
            sys.stderr.write(f"Tricks : adding calling entity birthdate sinc monthes matches !!")

    calculate_key(identity)

    entity = response.addEntity("reflets.Dirigeant", f"{identity['key']}")
    if identity['birthdate'] is not None : entity.addProperty( "date_naissance", "Naissance", "strict", identity['birthdate'] )
    if identity['birthdate_month'] is not None : entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", identity['birthdate_month'] )
    if identity['lastname'] is not None : entity.addProperty( "person.lastname", "Lastname", "loose", identity['lastname'] )
    if identity['firstname'] is not None : entity.addProperty( "person.firstnames", "Firstname", "loose", identity['firstname'] ) 
    if identity['firstnames'] is not None : entity.addProperty( "prenoms", "Prenoms", "loose", identity['firstnames'] )   
    if identity['age'] is not None : entity.addProperty( "age", "Age", "loose", identity['age'] )
    if identity['key'] is not None : entity.addProperty( "dirigeant", "Dirigeant", "loose", identity['key'] )           

    return entity


### Parse Representant from Pappers JSON and map it to Dirigeant Custom Maltego Entity
def parse_dirigeant( response , beneficiaire ) :

    # Location calculation
    identity = parse_identity(beneficiaire )

    entity = response.addEntity("reflets.Dirigeant", f"{identity['key']}")
    
    if identity['birthdate'] is not None : entity.addProperty( "date_naissance", "Naissance", "strict", identity['birthdate'] )
    if identity['birthdate_month'] is not None : entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", identity['birthdate_month'] )
    if identity['lastname'] is not None : entity.addProperty( "person.lastname", "Lastname", "loose", identity['lastname'] )
    if identity['firstname'] is not None : entity.addProperty( "person.firstnames", "Firstname", "loose", identity['firstname'] ) 
    if identity['firstnames'] is not None : entity.addProperty( "prenoms", "Prenoms", "loose", identity['firstnames'] )   
    if identity['age'] is not None : entity.addProperty( "age", "Age", "loose", identity['age'] )
    if identity['key'] is not None : entity.addProperty( "dirigeant", "Dirigeant", "loose", identity['key'] )           

    return entity


### Generate correct link configuration for REPRESENTANT
def generate_representant_link_config( entity , representant ) :
    if representant['qualite'] == 'Autre' : 
        link_label = 'Représentant'
    else :
        link_label = representant['qualite']

    if 'date_prise_de_poste' in representant and representant['date_prise_de_poste'] is not None :
        link_label += " en " + representant['date_prise_de_poste']

    entity.setLinkLabel(f"{link_label}")
    entity.reverseLink()
    entity.setLinkColor( "#657a8b" )   


### Generate correct link configuration for BENEFICIAIRE
def generate_beneficiaire_link_config( entity , beneficiaire ) :
    link_label = "Bénéficiaire"
    if 'pourcentage_parts' in beneficiaire and beneficiaire['pourcentage_parts'] is not None :
        link_label += f" : parts: {beneficiaire['pourcentage_parts']} ({beneficiaire['pourcentage_votes']})"
    entity.setLinkLabel( link_label )
    entity.setLinkColor( "#946b2d" )  


### Extract information related to the dirigeant and set the link with the correct message
def create_dirigeant_link_config ( entity , entreprise ) :
    if entreprise['dirigeant'] is not None:
        qualite = ""
        # Create the LINK LABEL : Relation du dirigeant à l'entreprise crée
        qualite = f"{entreprise['dirigeant']['qualites'][0]}"
        if qualite == "Autre" :
            qualite = "Dirigeant"

        #sys.stderr.write(f"{qualite_dirigeant} actuel depuis {entreprise['dirigeant']['date_prise_de_poste']}")
        if entreprise['dirigeant']['actuel'] is True :
            link_label = f"{qualite} actuel depuis {entreprise['dirigeant']['date_prise_de_poste']}"
        else :
            link_label = f"Ancien {qualite} arrivé {entreprise['dirigeant']['date_prise_de_poste']}"
            # Style : Dashed for old positions
            entity.setLinkStyle(1)

    entity.setLinkLabel(link_label)
    entity.setLinkColor( "#657a8b" )



### Parse societe from Pappers JSON and map it to DetailedCompany Custom Maltego Entity
def parse_entreprise( response , entreprise ):
        
        siren = normalize_siren( entreprise['siren'] )
        #sys.stderr.write(f"Entering entreprise parsing ")
        entity = response.addEntity("reflets.DetailedCompany", siren )

        entity.addProperty("id_tax_number","siren_vat","strict",siren)

        if 'libelle_code_naf' in entreprise :
            entity.addProperty("activity","Activity","loose",f"{entreprise['libelle_code_naf']}")

        if 'greffe' in entreprise :
            entity.addProperty("greffe","Greffe","loose",f"{entreprise['greffe']}")

        if 'numero_rcs' in entreprise :
            entity.addProperty("rcs","R.C.S","loose",f"{entreprise['numero_rcs']}")

        if 'numero_tva_intracommunautaire' in entreprise :
            entity.addProperty("tva","Num T.V.A","loose",f"{entreprise['numero_tva_intracommunautaire']}")

        if 'forme_juridique' in entreprise : 
            entity.addProperty("forme_juridique","Forme juridique","loose",f"{entreprise['forme_juridique']}")

        ### Difference for 'name' between long description (company details) and short one (representanst from company)
        if 'nom_entreprise' in entreprise : 
            entity.addProperty( "nom_usuel", "Nom", "loose", entreprise['nom_entreprise'] )
        elif 'nom_complet' in entreprise : 
            entity.addProperty( "nom_usuel", "Nom", "loose", entreprise['nom_complet'] )

        if 'date_creation' in entreprise :
            entity.addProperty("date_creation","Creation date","loose",f"{entreprise['date_creation']}")

        if 'date_cessation' in entreprise and entreprise['date_cessation'] is not None :
            entity.addProperty("date_cessation","Date cessation","loose",f"{entreprise['date_cessation']}")

            # Will add the color overlay in RED
            entity.addProperty("is_activ","Currently Activ","loose","#FF0000")  

        ### Location entity calculation
        if 'siege' in entreprise :
            location = parse_location(entity, entreprise['siege'])
        else :
            location = parse_location(entity, entreprise)

        if location['address'] is not None : entity.addProperty("headquarters_address","Siege","loose",f"{location['address']}")
        if location['city'] is not None : entity.addProperty("headquarters_city","Siege","loose",f"{location['city']}")
        if location['country'] is not None : entity.addProperty("country","Country code","loose",f"{location['country']}")
        if location['code_postal'] is not None : entity.addProperty("postalcode","Postal code","loose",f"{location['code_postal']}")        
        if location['country_code'] is None :
            sys.stderr.write(f"Cannot find country code.\n")
        else :   
            entity.addProperty("countrycode","Country Code","loose",location['country_code'])                

        return entity      


### Parse 'Depots Actes' NOTES
def parse_note( entity, json_res ) :
    note = ""
    config = load_api_key_config()

    siren = normalize_siren(json_res['siren'])

    note += "Extraits Pappers : " + f"https://api.pappers.fr/v2/document/extrait_pappers?siren={siren}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Extraits INPI : " + f"https://api.pappers.fr/v2/document/extrait_inpi?siren={siren}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Avis situation INSEE : " + f"https://api.pappers.fr/v2/document/avis_situation_insee?siren={siren}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Beneficiaires effectifs : " + f"https://api.pappers.fr/v2/document/declaration_beneficiaires_effectifs?siren={siren}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Dernier status : " + f"https://api.pappers.fr/v2/document/statuts?siren={siren}&api_token={config['pappers']['api_key']}" + "\n" 
    note += "Rapport Solvabilité : " + f"https://api.pappers.fr/v2/document/rapport_solvabilite?siren={siren}&api_token={config['pappers']['api_key']}" + "\n" 
    note += "\n\n"             

    if 'depots_actes' in json_res :
        note += "\nDépots actes : " + "\n"
        for acte in json_res['depots_actes']:

            if 'nom_fichier_pdf' in acte and acte['nom_fichier_pdf'] is not None :                        
                note += "  Name: " + acte['nom_fichier_pdf'] + "\n"
                note += "  URL : " + f"https://www.pappers.fr/document/telecharger?token={acte['token']}" + "\n"
            
            note += "  Date de dépot: " + acte['date_depot_formate'] + "\n"    

            if 'actes' in acte : 
                for decision in acte['actes']:
                    str = f"  {decision['type']} : {decision['decision']}"
                    note += str + "\n"

            note += "\n"
    
    if 'comptes' in json_res :
        note += "\nDépots comptes : " + "\n"        
        for acte in json_res['comptes']:

            if 'nom_fichier_pdf' in acte and acte['nom_fichier_pdf'] is not None :             
                note += "   Name: " + acte['nom_fichier_pdf'] + "\n"
                note += "   URL : " + f"https://www.pappers.fr/document/telecharger?token={acte['token']}" + "\n"
            if 'nom_fichier_xlsx' in acte and acte['nom_fichier_xlsx'] is not None : 
                note += "   Excel: " + acte['nom_fichier_xlsx'] + "\n" 
                note += "   URL : " + f"https://www.pappers.fr/document/telecharger?token={acte['token_xlsx']}" + "\n"              
            note += "   Date de dépot: " + acte['date_depot_formate'] + "\n"    

            note += "\n"

    if 'publications_bodacc' in json_res :
        note += "\nPublication bodacc : " + "\n" 
        for acte in json_res['publications_bodacc']:

            note += "   Numero: " + acte['numero_parution'] + "\n"                        
            note += "   Date: " + acte['date'] + "\n"
            note += "   Type: " + acte['type'] + "\n"
            note += "   Denomination: " + acte['denomination'] + "\n"            
            if 'descriptif' in acte and acte['descriptif'] is not None : 
                note += "   Descriptif: " + acte['descriptif'] + "\n" 
            if 'adresse' in acte and acte['adresse'] is not None : 
                note += "   Adresse: " + acte['adresse'] + "\n"                              

            note += "\n"

    entity.setNote(note)



## ENTITY parsing - IN


# Apply filters on search result to get only pertinent results
def do_filter_dirigeant_mention_in( request , dirigeant ) :

    # Identity calculation
    identity = parse_identity( dirigeant )

    try :
        if ( ( identity['firstname'] != request.getProperty('firstname') )or ( identity['lastname'] != request.getProperty('lastname') ) ) :
            sys.stderr.write(f"Dirigeant filtered : Names does'nt match : {dirigeant['first_name']} != {request.getProperty('firstname')} or {dirigeant['last_name']} != {request.getProperty('lastname')}\n")
            return 1

        # If we have birth_month in the calling entity and the results, we first check that      
        if (request.getProperty('date_naissance_rgpd') is not None ) and identity['birthdate_month'] is not None : 
            if ( request.getProperty('date_naissance_rgpd') != identity['birthdate_month'] ) :
                sys.stderr.write(f"Dirigeant filtered : Month Naissance does'nt match : {request.getProperty('date_naissance_rgpd')} != {identity['birthdate_month']} \n")
                return 1
            elif (request.getProperty('date_naissance') is not None ) and identity['birthdate'] is not None : 
                if ( request.getProperty('date_naissance') != identity['birthdate'] ) :
                    sys.stderr.write(f"Strange !! Dirigeant birth month match but not year (not filtered, LAZY) {request.getProperty('date_naissance')} != {identity['birthdate']} \n")

        else :
            #Default behaviour for date / month / age filtering : LAZY and let pass
            return 0
        
    except Exception as e : 
            sys.stderr.write(f"Error while applying filters {e}\n") 
    return 0 



### Parse societe from Pappers JSON and map it to DetailedCompany Custom Maltego Entity
def parse_company( response , entreprise, country_code ):
        #sys.stderr.write(f"Entering entreprise parsing ")
        siren = normalize_siren(entreprise['company_number'])        
        entity = response.addEntity("reflets.DetailedCompany", siren )

        entity.addProperty("id_tax_number","siren_vat","strict",siren)
        entity.addProperty("countrycode","Country Code","strict",country_code)        

        ### Difference for 'name' between long description (company details) and short one (representanst from company)
        if 'name' in entreprise : 
            entity.addProperty( "nom_usuel", "Nom", "loose", entreprise['name'] )  

        return entity      

### Extract information related to the dirigeant and set the link with the correct message
def create_officer_link_config ( entity , officer ) :
    link_label = ""
    if 'role' in officer and officer['role'] is not None :
        link_label += f"{officer['role']}"

    if 'type' in officer and officer['type'] is not None :
        link_label += f"({officer['type']})"

    if 'date_of_appointment' in officer and officer['date_of_appointment'] is not None :
        link_label += f" in {officer['date_of_appointment']}"

    # Style : Dashed for old positions
    entity.setLinkStyle(1)
    entity.setLinkLabel(link_label)
    entity.setLinkColor( "#657a8b" )





### Parse Representant from Pappers JSON and map it to Dirigeant Custom Maltego Entity
def parse_officers( response , beneficiaire ) :
    if 'company_name' in beneficiaire and beneficiaire['company_name'] is not None :

        siren = normalize_siren(beneficiaire['company_number'])
        entity = response.addEntity("reflets.DetailedCompany", siren )
        entity.addProperty("id_tax_number","siren_vat","strict",siren)
        entity.addProperty( "nom_usuel", "Nom", "loose", beneficiaire['company_name'] )

        # Location calculation
        location = parse_location(entity, beneficiaire )
        if location['address'] is not None : entity.addProperty("headquarters_address","Siege","loose",f"{location['address']}")
        if location['city'] is not None : entity.addProperty("headquarters_city","Siege","loose",f"{location['city']}")
        if location['country'] is not None : entity.addProperty("country","Country code","loose",f"{location['country']}")
        if location['code_postal'] is not None : entity.addProperty("postalcode","Postal code","loose",f"{location['code_postal']}")         
        if location['country_code'] is None :
            sys.stderr.write(f"Cannot find country code.\n")
        else :   
            entity.addProperty("countrycode","Country Code","loose",location['country_code'])                


    else :
        # Indentity calculation and normalization
        identity = parse_identity(beneficiaire )

        entity = response.addEntity("reflets.Dirigeant", f"{identity['key']}")
        
        if identity['birthdate'] is not None : entity.addProperty( "date_naissance", "Naissance", "strict", identity['birthdate'] )
        if identity['birthdate_month'] is not None : entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", identity['birthdate_month'] )
        if identity['lastname'] is not None : entity.addProperty( "person.lastname", "Lastname", "loose", identity['lastname'] )
        if identity['firstname'] is not None : entity.addProperty( "person.firstnames", "Firstname", "loose", identity['firstname'] ) 
        if identity['firstnames'] is not None : entity.addProperty( "prenoms", "Prenoms", "loose", identity['firstnames'] )   
        if identity['age'] is not None : entity.addProperty( "age", "Age", "loose", identity['age'] )
        if identity['key'] is not None : entity.addProperty( "dirigeant", "Dirigeant", "loose", identity['key'] )                

        entity.addProperty( "nationality", "Nationality", "loose", beneficiaire['nationality'] )

    return entity


### Extract information related to the dirigeant and set the link with the correct message
def create_officers_link_config ( entity , officers ) :
    link_label = ""
    # Create the LINK LABEL : Relation du dirigeant à l'entreprise crée
    if 'role' in officers and officers['role'] is not None : 
        link_label = f"{officers['role']}"

    #sys.stderr.write(f"{qualite_dirigeant} actuel depuis {entreprise['dirigeant']['date_prise_de_poste']}")
    if 'date_of_appointment' in officers and officers['date_of_appointment'] :
        link_label += f" in {officers['date_of_appointment']}"

    entity.setLinkLabel(link_label)
    entity.setLinkColor( "#657a8b" )



### Parse Etablissement
def parse_etablissement_in( response , etablissement, default_country_code ) :
    entity = response.addEntity("reflets.HeadquartersLocation", "test")

    ### Location entity calculation
    location = parse_location(entity, etablissement )
    if location['address'] is not None : entity.addProperty("streetaddress","Street Address","strict",f"{location['address']}")
    if location['city'] is not None : entity.addProperty("city","City","loose",f"{location['city']}")
    if location['country'] is not None : entity.addProperty("country","Country","loose",f"{location['country']}")
    if location['code_postal'] is not None : entity.addProperty("postalcode","Postal code","loose",f"{location['code_postal']}")     
    if location['country_code'] is None :
        sys.stderr.write(f"Cannot find country code in parse_etablissement_in\n")
        entity.addProperty("countrycode","Country code","loose",f"{default_country_code}")        
    else :   
        entity.addProperty("countrycode","Country code","loose",location['country_code'])                

    return entity

### Generate correct link configuration for HEADQUARTERS
def generate_siege_link_config_in( entity , etablissement ) :
    link_label = f"Current headoffices"
    entity.setLinkLabel(link_label)
    entity.setLinkThickness( 4 )



def parse_ubos ( response, ubos ) :

    # XXX Tricks to make the difference between individuals and companies, since pappers.in API does not support the info
    if ubos['date_of_birth'] is not None : 

        # Indentity calculation and normalization
        identity = parse_identity( ubos )

        entity = response.addEntity("reflets.Dirigeant", f"{identity['key']}")
        
        if identity['birthdate'] is not None : entity.addProperty( "date_naissance", "Naissance", "strict", identity['birthdate'] )
        if identity['birthdate_month'] is not None : entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", identity['birthdate_month'] )
        if identity['lastname'] is not None : entity.addProperty( "person.lastname", "Lastname", "loose", identity['lastname'] )
        if identity['firstname'] is not None : entity.addProperty( "person.firstnames", "Firstname", "loose", identity['firstname'] ) 
        if identity['firstnames'] is not None : entity.addProperty( "prenoms", "Prenoms", "loose", identity['firstnames'] )   
        if identity['age'] is not None : entity.addProperty( "age", "Age", "loose", identity['age'] )
        if identity['key'] is not None : entity.addProperty( "dirigeant", "Dirigeant", "loose", identity['key'] )                

        # XXX Calculate age ?????

    else :
        #sys.stderr.write(f"Entering entreprise parsing ")
        entity = response.addEntity("reflets.DetailedCompany", ubos['last_name'] )

        entity.addProperty( "nom_usuel", "Nom", "loose", ubos['last_name'] )

        # Location calculation
        location = parse_location(entity, ubos )
        if location['address'] is not None : entity.addProperty("headquarters_address","Siege","loose",f"{location['address']}")
        if location['city'] is not None : entity.addProperty("headquarters_city","Siege","loose",f"{location['city']}")
        if location['country'] is not None : entity.addProperty("country","Country code","loose",f"{location['country']}")
        if location['code_postal'] is not None : entity.addProperty("postalcode","Postal code","loose",f"{location['code_postal']}")          
        if location['country_code'] is None :
            sys.stderr.write(f"Cannot find country code.\n")
        else :   
            entity.addProperty("countrycode","Country Code","loose",location['country_code'])



    link_label = "Ubos "
    if 'percentage_of_shares' in ubos and ubos['percentage_of_shares'] is not None :
        link_label += f" : shares: {ubos['percentage_of_shares']}"
    entity.setLinkLabel( link_label )
    entity.setLinkColor( "#946b2d" )


    return entity




### Parse societe from Pappers JSON and map it to DetailedCompany Custom Maltego Entity
def parse_entreprise_in( response , entreprise ):
        #sys.stderr.write(f"Entering entreprise parsing ")
        siren = normalize_siren(entreprise['company_number'])
        entity = response.addEntity("reflets.DetailedCompany", siren )

        entity.addProperty("id_tax_number","siren_vat","strict",siren)

        if 'local_activities' in entreprise and len(entreprise['local_activities']) > 0 : 
            for act in entreprise['local_activities'] :
                entity.addProperty("activity","Activity","loose",f"{entreprise['name']}")
                break; 
        elif 'purpose' in entreprise :
            entity.addProperty("activity","Activity","loose",f"{entreprise['purpose']}")

        if 'local_legal_form_name' in entreprise : 
            entity.addProperty("forme_juridique","Forme juridique","loose",f"{entreprise['local_legal_form_name']}")


        ### Difference for 'name' between long description (company details) and short one (representanst from company)
        if 'name' in entreprise : 
            entity.addProperty( "nom_usuel", "Nom", "loose", entreprise['name'] )

        if 'date_of_creation' in entreprise :
            entity.addProperty("date_creation","Creation date","loose",f"{entreprise['date_of_creation']}")

        if 'date_of_cessation' in entreprise and entreprise['date_of_cessation'] is not None :
            entity.addProperty("date_cessation","Date cessation","loose",f"{entreprise['date_of_cessation']}")

            # Will add the color overlay in RED
            entity.addProperty("is_activ","Currently Activ","loose","#FF0000")  


        if 'head_office' in entreprise :

            # Location calculation
            location = parse_location(entity, entreprise['head_office'] )
            if location['address'] is not None : entity.addProperty("headquarters_address","Siege","loose",f"{location['address']}")
            if location['city'] is not None : entity.addProperty("headquarters_city","Siege","loose",f"{location['city']}")
            if location['country'] is not None : entity.addProperty("country","Country code","loose",f"{location['country']}")
            if location['code_postal'] is not None : entity.addProperty("postalcode","Postal code","loose",f"{location['code_postal']}")              
            if location['country_code'] is None :
                sys.stderr.write(f"Cannot find country code.\n")
            else :   
                entity.addProperty("countrycode","Country Code","loose",location['country_code'])

        return entity



### Parse 'Depots Actes' NOTES
def parse_note_in( entity, json_res ) :
    note = ""
    config = load_api_key_config()
 
    if 'documents' in json_res and len(json_res['documents']) > 0 :
        note += "\nDocuments : " + "\n"
        for acte in json_res['documents']:

            if 'type' in acte and acte['type'] is not None :                        
                note += "  Document type : " + acte['type'] + "\n"
            if 'description' in acte and acte['description'] is not None :                        
                note += "  Description : " + acte['description'] + "\n" 
            if 'date' in acte and acte['date'] is not None :                        
                note += "  Date : " + acte['date'] + "\n"

            if 'file_available' in acte and acte['file_available'] is True :                        
                note += "  URL : " + f"https://api.pappers.in/v1/download-file?api_token={config['pappers']['api_key']}&token={acte['file_token']}" + "\n"
            
            note += "\n"

    if 'financials' in json_res and len(json_res['financials']) > 0:
        note += "\nFinancials : " + "\n"
        for acte in json_res['financials']:

            if 'related_documents' in acte : 
                for doc in acte['related_documents'] :
                    if 'type' in doc and doc['type'] is not None :                        
                        note += "  Document type : " + doc['type'] + "\n"
                    if 'description' in doc and doc['description'] is not None :                        
                        note += "  Description : " + doc['description'] + "\n" 
                    if 'date' in acte and acte['date'] is not None :                        
                        note += "  Date : " + doc['date'] + "\n"

                    if 'file_available' in doc and doc['file_available'] is True :   
                        note += "  URL : " + f"https://api.pappers.in/v1/download-file?api_token={config['pappers']['api_key']}&token={doc['file_token']}" + "\n"
                    
                    note += "\n"                   

    if 'publications' in json_res and len(json_res['publications']) > 0 :
        note += "\nPublications : " + "\n"
        for acte in json_res['publications']:

            if 'type' in acte and acte['type'] is not None :                        
                note += "  Document type : " + acte['type'] + "\n"
            if 'description' in acte and acte['description'] is not None :                        
                note += "  Description : " + acte['description'] + "\n"
            elif 'content' in acte and acte['content'] is not None : 
                note += "  Description : " + acte['content'] + "\n"

            if 'date' in acte and acte['date'] is not None :                        
                note += "  Date : " + acte['date'] + "\n"

            if 'link' in acte and acte['link'] is not None :                        
                note += "  URL : " + acte['link'] + "\n"
            
            note += "\n"

    entity.setNote(note)
