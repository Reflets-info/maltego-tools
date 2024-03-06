import sys
import yaml
import requests

# PAPPERS API QUERYING


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


## ENTREPRISE SEARCH TERMS 
def create_payload_entreprise( request ) :
    config = load_api_key_config()

    payload_tpl = {}
    payload_tpl['api_token'] = config['pappers']['api_key']
    payload_tpl['siren'] = request.getProperty("id_tax_number")

    return payload_tpl


## BENEFICIAIRE SEARCH TERMS 
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
        payload_tpl['date_de_naissance_beneficiaire_min'] = (request.getProperty('date_naissance')).replace('/','-')
        payload_tpl['date_de_naissance_beneficiaire_max'] = (request.getProperty('date_naissance')).replace('/','-')

    # Create a age filter : thisnone seems to work
    if request.getProperty('age') is not None : 
        payload_tpl['age_beneficiaire_min'] = request.getProperty('age')
        payload_tpl['age_beneficiaire_max'] = request.getProperty('age')

    return payload_tpl


## BENEFICIAIRE SEARCH TERMS 
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
        payload_tpl['date_de_naissance_dirigeant_min'] = (request.getProperty('date_naissance')).replace('/','-')
        payload_tpl['date_de_naissance_dirigeant_max'] = (request.getProperty('date_naissance')).replace('/','-')

    # Create a age filter : thisnone seems to work
    if request.getProperty('age') is not None : 
        payload_tpl['age_dirigeant_min'] = request.getProperty('age')
        payload_tpl['age_dirigeant_max'] = request.getProperty('age')

    return payload_tpl


# Apply filters on search result to get only pertinent results
def do_filter_dirigeant_entity( request , dirigeant ) :
    # Filter to remove noise from search result
    prenom_filter = ""
    if ( 'prenom_usuel' in dirigeant and dirigeant['prenom_usuel'] is not None ) :
        prenom_filter = dirigeant['prenom_usuel']

    nom_filter = ""
    if ('nom' in dirigeant and dirigeant['nom'] is not None) : 
        nom_filter = dirigeant['nom']

    date_filter = ""
    if 'date_de_naissance_formate' in dirigeant and dirigeant['date_de_naissance_formate'] is not None :
        date_filter = dirigeant['date_de_naissance_formate']

    month_filter = ""
    if 'date_de_naissance_formatee' in dirigeant and dirigeant['date_de_naissance_formatee'] is not None :
        month_filter = dirigeant['date_de_naissance_formatee']

    age_filter = ""
    if 'age' in dirigeant and dirigeant['age'] is not None :
        age_filter = dirigeant['age']       

    try :
        if (
            ( prenom_filter.casefold() != request.getProperty('firstname').casefold() )
            or 
            ( nom_filter.casefold().replace("-"," ") != request.getProperty('lastname').casefold().replace("-"," ") )
            ) :
            sys.stderr.write(f"Dirigeant filtered : Names does'nt match : {dirigeant['prenom_usuel']} != {request.getProperty('firstname')} or {dirigeant['nom']} != {request.getProperty('lastname')}\n")
            return 1

        # If we have birth_month in the calling entity and the results, we first check that      
        if (request.getProperty('date_naissance_rgpd') is not None ) and month_filter != "" : 
            if ( request.getProperty('date_naissance_rgpd') != month_filter ) :
                sys.stderr.write(f"Dirigeant filtered : Month Naissance does'nt match : {request.getProperty('date_naissance_rgpd')} != {month_filter} \n")
                return 1
            elif (request.getProperty('date_naissance') is not None ) and date_filter != "" : 
                if ( request.getProperty('date_naissance') != date_filter ) :
                    sys.stderr.write(f"Strange !! Dirigeant birth month match but not year (not filtered, LAZY) {request.getProperty('date_naissance')} != {date_filter} \n")

        # If we have matched month, we keep the entity
        # If not, we try to match on pirth date  
        elif (request.getProperty('date_naissance') is not None ) and date_filter != "" : 
                if ( request.getProperty('date_naissance') != date_filter ) :
                    sys.stderr.write(f"Dirigeant filtered : Date naissance doesn't match {request.getProperty('date_naissance')} != {date_filter} | {request.getProperty('date_naissance_rgpd')} != {month_filter}\n")
                    return 1

        elif request.getProperty('age') is not None and age_filter != "" :
            if int(request.getProperty('age')) != age_filter :
                sys.stderr.write(f"Dirigeant filtered : Age does not match : {request.getProperty('age')} != {age_filter}\n")
                return 1 
        else :
            #Default behaviour for date / month / age filtering : LAZY and let pass
            return 0
        
    except Exception as e : 
            sys.stderr.write(f"Error while applying filters {e}\n") 
    return 0 




# PAPPERS JSON RESULT PARSING AND MALTEGO ENTITY CREATION

## ENTITY parsing

### Parse Etablissement
def parse_etablissement( response , etablissement ) :
    entity = response.addEntity("reflets.HeadquartersLocation", "test")
    entity.addProperty("streetaddress","Street Address","strict",etablissement['adresse_ligne_1'])
    entity.addProperty("city","City","loose",etablissement['ville'])
    entity.addProperty("country","Contry","loose",etablissement['pays'])
    entity.addProperty("countrycode","Contry code","loose",etablissement['code_pays'])
    entity.addProperty("activity","Activity","loose",etablissement['libelle_code_naf']) 
    return entity

### Parse Representant from Pappers JSON and map it to Dirigeant Custom Maltego Entity
def parse_dirigeant( response , beneficiaire ) :
    entity = response.addEntity("reflets.Dirigeant", f"{beneficiaire['prenom_usuel']} {beneficiaire['nom']}")

    # XXX Handling differents parameter names in the Pappers API
    if 'date_de_naissance_formatee' in beneficiaire : 
        entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", beneficiaire['date_de_naissance_formatee'] )
    elif 'date_de_naissance_rgpd_formatee' in beneficiaire : 
        entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", beneficiaire['date_de_naissance_rgpd_formatee'] )

    if 'date_de_naissance_complete_formatee' in beneficiaire : 
        entity.addProperty( "date_naissance", "Naissance", "strict", beneficiaire['date_de_naissance_complete_formatee'] )
    elif 'date_de_naissance_formate' in beneficiaire : 
        entity.addProperty( "date_naissance", "Naissance", "strict", beneficiaire['date_de_naissance_formate'] )

    entity.addProperty( "prenoms", "Prenoms", "loose", beneficiaire['prenom'] )
    entity.addProperty( "person.firstnames", "Firstname", "loose", beneficiaire['prenom_usuel'] )
    entity.addProperty( "person.lastname", "Lastname", "loose", beneficiaire['nom'] )
    if 'age' in beneficiaire : 
        entity.addProperty( "age", "Age", "loose", beneficiaire['age'] )

    return entity


### Parse societe from Pappers JSON and map it to DetailedCompany Custom Maltego Entity
def parse_entreprise( response , entreprise ):
        #sys.stderr.write(f"Entering entreprise parsing ")
        entity = response.addEntity("reflets.DetailedCompany", entreprise['siren'] )

        entity.addProperty("id_tax_number","siren_vat","strict",entreprise['siren'])

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

        if 'siege' in entreprise :

            entity.addProperty("headquarters_address","Siege","loose",f"{entreprise['siege']['adresse_ligne_1']}")
            entity.addProperty("headquarters_city","Siege","loose",f"{entreprise['siege']['ville']}")

            # Will put the little flag 
            if 'code_pays' in entreprise['siege'] : 
                entity.addProperty("countrycode","Country Code","loose",entreprise['siege']['code_pays']) 
            elif 'pays' in entreprise['siege'] :
                if entreprise['siege']['pays'] == 'France' :  
                    entity.addProperty("countrycode","Country Code","loose",'FR') 
                elif entreprise['siege']['pays'] == 'ETATS-UNIS' :
                    entity.addProperty("countrycode","Country Code","loose",'US')
                else :
                    sys.stderr.write(f"Cannot find country code. Maybe add {entreprise['siege']['pays']} to the mapping\n")
            else :
                sys.stderr.write(f"Unable to detect country code for Entreprise {entreprise['siren']}.\n")

        ### Parsing short 'representants' description in company details call
        else :
            if 'adresse_ligne_1' in entreprise :
                entity.addProperty("headquarters_address","Siege","loose",f"{entreprise['adresse_ligne_1']}")
            if 'ville' in entreprise :
                entity.addProperty("headquarters_city","Siege","loose",f"{entreprise['ville']}")

            # Will put the little flag 
            if 'code_pays' in entreprise :
                entity.addProperty("countrycode","Country Code","loose",entreprise['code_pays']) 

        return entity      


### Parse 'Depots Actes' NOTES
def parse_note( entity, json_res ) :
    note = ""
    config = load_api_key_config()

    note += "Extraits Pappers : " + f"https://api.pappers.fr/v2/document/extrait_pappers?siren={json_res['siren']}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Extraits INPI : " + f"https://api.pappers.fr/v2/document/extrait_inpi?siren={json_res['siren']}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Avis situation INSEE : " + f"https://api.pappers.fr/v2/document/avis_situation_insee?siren={json_res['siren']}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Beneficiaires effectifs : " + f"https://api.pappers.fr/v2/document/declaration_beneficiaires_effectifs?siren={json_res['siren']}&api_token={config['pappers']['api_key']}" + "\n"
    note += "Dernier status : " + f"https://api.pappers.fr/v2/document/statuts?siren={json_res['siren']}&api_token={config['pappers']['api_key']}" + "\n" 
    note += "Rapport Solvabilité : " + f"https://api.pappers.fr/v2/document/rapport_solvabilite?siren={json_res['siren']}&api_token={config['pappers']['api_key']}" + "\n" 
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




## LINKS config creation

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

