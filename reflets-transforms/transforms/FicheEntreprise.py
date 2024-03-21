# This Maltego Transformer is designed to explore ENTREPRISES from the Pappers.fr API and build corresponding entities

import json
import sys
from transforms import papperparse
import os

import requests

from extensions import registry
from maltego_trx.entities import Company
from maltego_trx.maltego import UIM_TYPES, MaltegoMsg, MaltegoTransform
from maltego_trx.transform import DiscoverableTransform


# Parse a DetailedCompany from the results of the Pappers FR V2 API
def parse_entreprise_fr ( response, json_res ) : 

    # Creation of new node Dirigeant for Beneficiaries
    for beneficiaire in json_res['beneficiaires_effectifs']:
        try : 
            #sys.stderr.write(f" New beneficiare {beneficiaire['prenom_usuel']} {beneficiaire['nom']} with qualité {beneficiaire['pourcentage_parts']}\n") 
            entity = papperparse.parse_dirigeant( response, beneficiaire)
            papperparse.generate_beneficiaire_link_config( entity, beneficiaire )

        except Exception as e :
            sys.stderr.write(f"Error: {e}\n")
            sys.stderr.write(f"Beneficiaire: {json.dumps(beneficiaire , indent=4)}\n")


    for representant in json_res['representants']:
        try : 
            # Creation of a DIRIGEANT node for a private individual
            if representant['personne_morale'] == False : 
                #sys.stderr.write(f" Representant personne physique {representant['prenom_usuel']} {representant['nom']} with qualité {representant['qualite']}\n")
                entity = papperparse.parse_dirigeant( response , representant )
                papperparse.generate_representant_link_config( entity , representant )
                    
            # Creation of a DETAILED_COMPANY node for a legal entity 
            else :
                entity = papperparse.parse_entreprise( response, representant) 
                papperparse.generate_representant_link_config( entity , representant )

        except Exception as e :
            sys.stderr.write(f"Error: {e}\n")
            sys.stderr.write(f"Representant: {json.dumps(representant , indent=4)}\n")


    # Parsing of headquarters
    if 'siege' in json_res :
        try : 
            entity = papperparse.parse_etablissement( response, json_res['siege'] )
            papperparse.generate_siege_link_config( entity , json_res['siege'] )
        except Exception as e :
            sys.stderr.write(f"Error: {e}\n")
            sys.stderr.write(f"Siege: {json.dumps(json_res['siege'], indent=4)}\n")


    # Extract 'etablissements' to build HeadquarterLocation
    # When merged, it may help to understand links between compagnies
    for etablissement in json_res['etablissements']:
        try : 
            entity = papperparse.parse_etablissement( response, etablissement )
            papperparse.generate_etablissement_link_config( entity , etablissement )
        except Exception as e :
            sys.stderr.write(f"Error: {e}\n")
            sys.stderr.write(f"Etablissement: {json.dumps(etablissement, indent=4)}\n")


    # Auto-qualification of the calling entity to be able to update the note on the current node.
    try : 
        entity = papperparse.parse_entreprise( response , json_res )            
        # Auto-qualification : generation of a note with documents and download links 
        papperparse.parse_note(entity, json_res )
    except Exception as e :
        sys.stderr.write(f"Error: {e}\n")
        sys.stderr.write(f"Problem in the main result parsing for auto-qualification\n")


# Parsing results of the Pappers IN V1 API for a DetailedCompany
def parse_entreprise_in ( response, json_res, default_country_code ) : 
                
    # Creation of new node Dirigeant for Beneficiaries
    for officers in json_res['officers']:
        try : 
            #sys.stderr.write(f" New beneficiare {beneficiaire['prenom_usuel']} {beneficiaire['nom']} with qualité {beneficiaire['pourcentage_parts']}\n") 
            entity = papperparse.parse_officers( response, officers)
            papperparse.create_officers_link_config( entity, officers )

        except Exception as e :
            sys.stderr.write(f"Error: {e}\n")
            sys.stderr.write(f"Beneficiaire: {json.dumps(officers , indent=4)}\n")

    # Parsing of headquarters
    if 'head_office' in json_res :
        try : 
            entity = papperparse.parse_etablissement_in( response, json_res['head_office'], default_country_code )
            papperparse.generate_siege_link_config_in( entity , json_res['head_office'] )
        except Exception as e :
            sys.stderr.write(f"Error: {e}\n")
            sys.stderr.write(f"Siege: {json.dumps(json_res['head_office'], indent=4)}\n")


    # Creation of new node Dirigeant for Beneficiaries
    for ubos in json_res['ubos']:
        try : 
            entity = papperparse.parse_ubos(response, ubos )

        except Exception as e :
            sys.stderr.write(f"Error: {e}\n")
            sys.stderr.write(f"Representant: {json.dumps(ubos , indent=4)}\n")


    # Auto-qualification of the calling entity to be able to update the note on the current node.
    try : 
        entity = papperparse.parse_entreprise_in( response , json_res )            
        # Auto-qualification : generation of a note with documents and download links 
        papperparse.parse_note_in(entity, json_res )
    except Exception as e :
        sys.stderr.write(f"Error: {e}\n")
        sys.stderr.write(f"Problem in the main result parsing for auto-qualification\n")



@registry.register_transform(display_name="Pappers.fr - Fiche Entrerise", input_entity="reflets.DetailedCompany",
                             description='Pappers.fr - Fiche Entrerise',
                             output_entities=["maltego.Document", "maltego.Person"])
class FicheEntreprise(DiscoverableTransform):

    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):
        try:

            country_code = request.getProperty('countrycode')
            if country_code is not None and ( country_code == 'CH' or country_code == 'UK' or country_code == 'GB' or country_code == 'BE') :
                payload = papperparse.create_payload_entreprise(request)

                # XXX For compatibility with Maltego country code and flags
                if country_code == 'GB' :
                     payload['country_code'] = 'UK'
                else :
                    payload['country_code'] = country_code

                payload['company_number'] = payload['siren']
                payload['fields'] = 'officers,ubos,financials,documents,certificates,publications,establishments,contacts'
                json_res = papperparse.make_request("https://api.pappers.in/v1/company", payload)
                #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}")

                parse_entreprise_in( response, json_res, country_code )

                return                 

            payload = papperparse.create_payload_entreprise(request)
            json_res = papperparse.make_request("https://api.pappers.fr/v2/entreprise", payload)
            #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}") 

            parse_entreprise_fr( response, json_res )

        except Exception as e:
            response.addUIMessage(f"Error: {e}\n")

