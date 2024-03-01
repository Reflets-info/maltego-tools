# This Maltego Transformer is designed to explore ENTREPRISES from the Pappers.fr API and build corresponding entities

import json
import pickle
import sys
import yaml

import requests

from extensions import registry
from maltego_trx.entities import Company
from maltego_trx.maltego import UIM_TYPES, MaltegoMsg, MaltegoTransform
from maltego_trx.transform import DiscoverableTransform


@registry.register_transform(display_name="Fiche company", input_entity="reflets.DetailedCompany",
                             description='Get detailed info about company',
                             output_entities=["maltego.Document", "maltego.Person"])
class CompanyPappers(DiscoverableTransform):

    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):
        #print(f"Request: {request}")
        #str = pickle.dumps(request)
        #print(f"{request.Value}")
        siren = request.getProperty("id_tax_number")

        try:
            # DIRIGEANT SEARCH TERMS 
            # Using most precise key info to get the good guy
            payload = {}
            with open('./transforms/api_keys.yml', 'r') as file :
              config = yaml.safe_load(file)
            
            payload['api_token'] = config['pappers']['api_key']
            payload['siren'] = siren

            page = requests.get("https://api.pappers.fr/v2/entreprise", params=payload)
            if page.status_code == 401:
                raise Exception("Bad API key")
            elif page.status_code == 404:
                raise Exception("No results !")
            elif page.status_code == 503:
                raise Exception("Service unavailable : try again later")
            elif page.status_code == 200:
                json_res = page.json()
                #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}") 

                # Creation of new node Dirigeant for Beneficiaries
                for beneficiaire in json_res['beneficiaires_effectifs']:
                    sys.stderr.write(f" New beneficiare {beneficiaire['prenom_usuel']} {beneficiaire['nom']} with qualité {beneficiaire['pourcentage_parts']}\n")
                    entity = response.addEntity("reflets.Dirigeant", f"{beneficiaire['prenom_usuel']} {beneficiaire['nom']}")
                    entity.setLinkLabel(f"Parts: {beneficiaire['pourcentage_parts']} / Votes: {beneficiaire['pourcentage_votes']}")
                    entity.setLinkColor( "#946b2d" )

                    entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", beneficiaire['date_de_naissance_formatee'] )
                    entity.addProperty( "date_naissance", "Naissance", "strict", beneficiaire['date_de_naissance_complete_formatee'] )
                    entity.addProperty( "prenoms", "Prenoms", "loose", beneficiaire['prenom'] )
                    entity.addProperty( "person.firstnames", "Firstname", "loose", beneficiaire['prenom_usuel'] )
                    entity.addProperty( "person.lastname", "Lastname", "loose", beneficiaire['nom'] )
                    if 'age' in beneficiaire : 
                        entity.addProperty( "age", "Age", "loose", beneficiaire['age'] )

                for representant in json_res['representants']:

                    # Creation of a DIRIGEANT node for a private individual
                    if representant['personne_morale'] == False : 
                        #sys.stderr.write(f" Representant personne physique {representant['prenom_usuel']} {representant['nom']} with qualité {representant['qualite']}\n")
                        entity = response.addEntity("reflets.Dirigeant", f"{representant['prenom_usuel']} {representant['nom']}")
 
                        link_label = representant['qualite']
                        if 'date_prise_de_poste' in representant and representant['date_prise_de_poste'] is not None :
                            link_label += " en " + representant['date_prise_de_poste']
                        entity.setLinkLabel(f"{link_label}")
                        entity.reverseLink()
                        entity.setLinkColor( "#657a8b" )
 
                        entity.addProperty( "date_naissance_rgpd", "Naissance RGPD", "strict", representant['date_de_naissance_rgpd_formatee'] )
                        entity.addProperty( "date_naissance", "Naissance", "strict", representant['date_de_naissance_formate'] )
                        entity.addProperty( "prenoms", "Prenoms", "loose", representant['prenom'] )
                        entity.addProperty( "person.firstnames", "Firstname", "loose", representant['prenom_usuel'] )
                        entity.addProperty( "person.lastname", "Lastname", "loose", representant['nom'] )
                        if 'age' in representant : 
                            entity.addProperty( "age", "Age", "loose", representant['age'] )
                            
                    # Creation of a DETAILED_COMPANY node for a legal entity 
                    else :
                        #sys.stderr.write(f" Representant personne morale {representant['nom_complet']} {representant['siren']}\n")
                        entity = response.addEntity("reflets.DetailedCompany", f"{representant['siren']}")

                        link_label = representant['qualite']
                        if 'date_prise_de_poste' in representant and representant['date_prise_de_poste'] is not None :
                            link_label += " en " + representant['date_prise_de_poste']
                        entity.setLinkLabel(f"{link_label}")
                        entity.reverseLink()
                        entity.setLinkColor( "#657a8b" )

                        entity.addProperty( "id_tax_number", "siren_vat", "strict", representant['siren'] )
                        entity.addProperty( "nom_usuel", "Nom", "loose", representant['nom_complet'] )

                # Extract 'etablissements' to build HeadquarterLocation
                # When merged, it may help to understand links between compagnies
                for etablissement in json_res['etablissements']:
                    #sys.stderr.write(f" New representant {representant['prenom_usuel']} {representant['nom']} with qualité {representant['qualite']}\n")
                    entity = response.addEntity("reflets.HeadquartersLocation", "test")
                    entity.addProperty("streetaddress","Street Address","strict",etablissement['adresse_ligne_1'])
                    entity.addProperty("city","City","loose",etablissement['ville'])
                    entity.addProperty("country","Contry","loose",etablissement['pays'])

                    link_label = ""
                    if etablissement['date_cessation'] is not None :
                        link_label = f"Entre {etablissement['date_de_creation']} et {etablissement['date_cessation']}"
                    else :
                        link_label = f"Depuis {etablissement['date_de creation']}"
                    entity.setLinkLabel(link_label)


                # Auto-qualification of the calling entity to be able to update the note on the current node.
                entity = response.addEntity("reflets.DetailedCompany", f"{request.Value}" )
                entity.addProperty( "id_tax_number", "siren_vat", "loose", siren )
                entity.addProperty( "activity" ,"Activity","loose",f"{json_res['libelle_code_naf']}")
                entity.addProperty( "date_creation","Creation date","loose",f"{json_res['date_creation']}")
                entity.addProperty( "forme_juridique","Forme juridique","loose",f"{json_res['forme_juridique']}")
                if 'date_cessation' in json_res and json_res['date_cessation'] is not None :
                    entity.addProperty("date_cessation","Date cessation","loose",f"{json_res['date_cessation']}")                

                # Auto-qualification : generation of a note with documents and download links 
                if 'depots_actes' in json_res :

                    note = ""
                    for acte in json_res['depots_actes']:
                        
                        note += "Name: " + acte['nom_fichier_pdf'] + "\n"
                        note += "Date de dépot: " + acte['date_depot_formate'] + "\n"    
                        # URL format : https://www.pappers.fr/document/telecharger?token=QTcwNTg1NDM3MDAwMDAwX0MwMDIyQTEwMDFMNTY3NzQ5RDIwMTYwMTA2SDE4MDY1N1RQSUpURVMwMDNQREJPUg
                        url = f"https://www.pappers.fr/document/telecharger?token={acte['token']}"
                        note += "URL : " + url + "\n"

                        if 'actes' in acte : 
                            for decision in acte['actes']:
                                str = f"{decision['type']} : {decision['decision']}"
                                note += str + "\n"

                        note += "\n"

                    entity.setNote(note)
            else:
                raise Exception("Unknown error code !")

        except Exception as e:
            response.addUIMessage(f"Error: {e}")

        # Write the slider value as a UI message - just for fun
        response.addUIMessage(f"Slider value is at: {request.Slider}")
