# This Maltego Transformer is designed to explore ENTREPRISES from the Pappers.fr API and build corresponding entities

import json
import pickle
import sys
import yaml
from transforms import papperparse
import os

import requests

from extensions import registry
from maltego_trx.entities import Company
from maltego_trx.maltego import UIM_TYPES, MaltegoMsg, MaltegoTransform
from maltego_trx.transform import DiscoverableTransform


@registry.register_transform(display_name="Pappers.fr - Fiche Entrerise", input_entity="reflets.DetailedCompany",
                             description='Pappers.fr - Fiche Entrerise',
                             output_entities=["maltego.Document", "maltego.Person"])
class FicheEntreprise(DiscoverableTransform):

    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):
        try:

            payload = papperparse.create_payload_entreprise(request)
            json_res = papperparse.make_request("https://api.pappers.fr/v2/entreprise", payload)
            #sys.stderr.write(f"Response: {json.dumps(json_res, indent=4)}") 

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


        except Exception as e:
            response.addUIMessage(f"Error: {e}\n")

