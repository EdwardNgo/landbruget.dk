import requests
import json
import logging
import builtins

# Configure logging and override print to use logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Redirect all print calls in this module to logger.info
builtins.print = logger.info


class CVRScraper:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.base_url = "https://api.cvr.dk/cvr/api/v1"
        self.metadata_api_endpoint = "http://distribution.virk.dk/cvr-permanent/virksomhed/_search"
        self.publication_api_endpoint = ""
        
    def retrieve_metadata(self, cvr_to_query, fetch_all_fields=True):
        # Define the data fields you want to retrieve IF fetch_all_fields is False
        # Refer to the 'cvr-indeks_data_katalog (2).docx' for available fields
        fields_to_retrieve = [
            "Vrvirksomhed.cvrNummer",
            "Vrvirksomhed.virksomhedMetadata.nyesteNavn.navn",
            "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse",
            "Vrvirksomhed.virksomhedsform", # Note: Fetching the whole object now
            "Vrvirksomhed.nyesteHovedbranche", # Note: Fetching the whole object now
            "Vrvirksomhed.livsforloeb",
            "Vrvirksomhed.status"
        ]

        # Construct the ElasticSearch query payload
        query_payload = {
        # "_source" filter is only included if fetch_all_fields is False
        "query": {
            "term": {
            "Vrvirksomhed.cvrNummer": cvr_to_query
            }
        }
        }

        # Add the source filter only if fetch_all_fields is False
        if not fetch_all_fields:
            query_payload["_source"] = fields_to_retrieve
            print("Fetching specific fields only.")
        else:
            print("Fetching ALL available fields for this CVR.")


        # Define the necessary headers
        headers = {
            'Content-Type': 'application/json'
        }

        try:
            # Make the POST request with authentication
            response = requests.post(
                self.metadata_api_endpoint,
                auth=(self.username, self.password),
                headers=headers,
                json=query_payload
            )

            # Check if the request was successful
            response.raise_for_status()

            # Parse the JSON response
            data = response.json()

            print("\n--- API Response ---")
            # Pretty print the JSON data
            print(json.dumps(data, indent=2, ensure_ascii=False))

            # Process the response if hits were found
            if data.get("hits", {}).get("total", 0) > 0:
                # Get the main data object for the first hit
                # Use .get() with default empty dict {} to avoid errors if structure is unexpected
                hit_source = data.get("hits", {}).get("hits", [{}])[0].get("_source", {})
                vr_virksomhed = hit_source.get("Vrvirksomhed", {})

                print("\n--- Extracted Information ---")

                # Safely get CVR Number
                cvr_num = vr_virksomhed.get('cvrNummer', 'N/A')
                print(f"CVR Number: {cvr_num}")

                # Safely get Company Name
                # Chain .get() calls to navigate the structure safely
                company_name = vr_virksomhed.get('virksomhedMetadata', {}).get('nyesteNavn', {}).get('navn', 'N/A')
                print(f"Company Name: {company_name}")

                # Safely get Company Type (handle list)
                company_type_list = vr_virksomhed.get('virksomhedsform', []) # Default to empty list
                if company_type_list and isinstance(company_type_list, list) and len(company_type_list) > 0:
                    # Access first item (dict) safely and get the description
                    company_type = company_type_list[0].get('kortBeskrivelse', 'N/A')
                else:
                    company_type = 'N/A'
                print(f"Company Type: {company_type}")

                # Safely get Address
                address_obj = vr_virksomhed.get('virksomhedMetadata', {}).get('nyesteBeliggenhedsadresse', {})
                if address_obj: # Check if address object exists and is not empty
                    addr_parts = [
                        address_obj.get('vejnavn', ''),
                        address_obj.get('husnummerFra', ''),
                        address_obj.get('bogstavFra', ''),
                        f"({address_obj.get('etage', '')})" if address_obj.get('etage') else '',
                        f"({address_obj.get('sidedoer', '')})" if address_obj.get('sidedoer') else '',
                        ',',
                        address_obj.get('postnummer', ''),
                        address_obj.get('postdistrikt', ''),
                        f"({address_obj.get('bynavn', '')})" if address_obj.get('bynavn') else '',
                        address_obj.get('landekode', '')
                    ]
                    # Filter out empty strings and join
                    full_address = ' '.join(part for part in addr_parts if part).replace(' ,', ',')
                    print(f"Address: {full_address}")
                else:
                    print("Address: N/A")

                # Safely get Industry
                industry_obj = vr_virksomhed.get('nyesteHovedbranche', {})
                if industry_obj:
                    industry_text = industry_obj.get('branchetekst', 'N/A')
                    industry_code = industry_obj.get('branchekode', 'N/A')
                    print(f"Industry: {industry_text} (Code: {industry_code})")
                else:
                    print("Industry: N/A (Not found in response)")

                # Get Status Codes (handle list)
                status_list = vr_virksomhed.get('status', [])
                if status_list:
                    print(f"Status Codes: {status_list}")
                else:
                    print("Status Codes: [] (None reported)")

                # Get Lifecycle info (handle list)
                lifecycle_list = vr_virksomhed.get('livsforloeb', [])
                if lifecycle_list:
                    print("Lifecycle Events:")
                    for event in lifecycle_list:
                        period = event.get('periode', {})
                        start = period.get('gyldigFra', 'N/A')
                        end = period.get('gyldigTil', 'N/A')
                        updated = event.get('sidstOpdateret', 'N/A')
                        print(f"  - Period: {start} to {end} (Last Updated: {updated})")
                else:
                    print("Lifecycle Events: N/A")

            else:
                print(f"\nNo company found with CVR number: {cvr_to_query}")


        except requests.exceptions.HTTPError as http_err:
            print(f"\nHTTP Error occurred: {http_err}")
            print(f"Response Content: {response.text}") # Print response body for debugging
            if response.status_code == 401:
                print("Authentication failed (Unauthorized). Please check your username and password.")
            elif response.status_code == 403:
                print("Access forbidden. Ensure your user has permissions for this index.")
        except requests.exceptions.ConnectionError as conn_err:
            print(f"\nConnection Error occurred: {conn_err}")
            print("Could not connect to the API endpoint. Check the URL and your network connection.")
        except requests.exceptions.Timeout as timeout_err:
            print(f"\nTimeout Error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"\nAn unexpected error occurred: {req_err}")
        except Exception as e:
            # Print the actual error for better debugging
            import traceback
            print(f"\nAn error occurred during script execution: {e}")
            print("Traceback:")
            traceback.print_exc()
            
    def retrieve_publication(self, cvr_to_query, max_results = 10):
        query_payload = {
            "size": max_results,
            "query": {
                "term": {
                "cvrNummer": cvr_to_query
                }
            },
            "sort": [
                {
                "offentliggoerelsesTidspunkt": {
                    "order": "desc"
                }
                }
            ]
        }

        # Define the necessary headers
        headers = {
            'Content-Type': 'application/json'
        }

        print(f"Querying API for documents related to CVR: {cvr_to_query}...")
        print(f"Using endpoint: {api_url}")
        print(f"Fetching up to {max_results} results.")

        try:
            # Make the POST request with authentication
            response = requests.post(
                api_url,
                auth=(cvr_username, cvr_password),
                headers=headers,
                json=query_payload
            )

            # Check if the request was successful
            response.raise_for_status()

            # Parse the JSON response
            data = response.json()

            # Optional: Print the raw response again if needed for debugging
            # print("\n--- FULL API Response (Documents Found) ---")
            # print(json.dumps(data, indent=2, ensure_ascii=False))

            # --- Corrected Extraction Logic ---
            if data.get("hits", {}).get("total", 0) > 0:
                print(f"\n--- Corrected Extracted Document Information (Newest {max_results} first) ---")
                # Iterate through each publication event found
                for i, hit in enumerate(data.get("hits", {}).get("hits", [])):
                    doc_source = hit.get("_source", {}) # Get the full source for this publication

                    print(f"\nPublication Event {i+1}:")

                    # Extract top-level info for the event
                    pub_type = doc_source.get("offentliggoerelsestype", "N/A")
                    pub_time = doc_source.get("offentliggoerelsesTidspunkt", "N/A")
                    sags_nr = doc_source.get("sagsNummer", "N/A")
                    print(f"  Publication Type: {pub_type}")
                    print(f"  Publication Time: {pub_time}")
                    print(f"  Case Number: {sags_nr}")

                    # Extract reporting period info (nested)
                    regnskab_info = doc_source.get('regnskab', {})
                    periode_info = regnskab_info.get('regnskabsperiode', {})
                    period_start = periode_info.get("startDato", "N/A")
                    period_end = periode_info.get("slutDato", "N/A")
                    print(f"  Reporting Period: {period_start} to {period_end}")

                    # Extract info for EACH document within this publication event
                    dokumenter_list = doc_source.get('dokumenter', []) # Get the list of documents
                    if dokumenter_list:
                        print(f"  Documents included ({len(dokumenter_list)}):")
                        for j, doc in enumerate(dokumenter_list):
                            doc_url = doc.get("dokumentUrl", "N/A")
                            mime_type = doc.get("dokumentMimeType", "N/A")
                            doc_type = doc.get("dokumentType", "N/A") # Should be 'AARSRAPPORT'
                            print(f"    - Document {j+1}:")
                            print(f"      Type: {doc_type}")
                            print(f"      Mime Type: {mime_type}")
                            print(f"      URL: {doc_url}") # <-- This is the link!
                    else:
                        print("  No documents found within this publication event.")

            else:
                print(f"\nNo documents found for CVR number: {cvr_to_query} via this endpoint.")


        except requests.exceptions.HTTPError as http_err:
            print(f"\nHTTP Error occurred: {http_err}")
            print(f"Response Content: {response.text}")
            if response.status_code == 401:
                print("Authentication failed (Unauthorized). Please check your username and password.")
            elif response.status_code == 403:
                print("Access forbidden. Ensure your user has permissions for this index ('offentliggoerelser' or similar).")
        except requests.exceptions.ConnectionError as conn_err:
            print(f"\nConnection Error occurred: {conn_err}")
            print("Could not connect to the API endpoint. Check the URL and your network connection.")
        except requests.exceptions.Timeout as timeout_err:
            print(f"\nTimeout Error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"\nAn unexpected error occurred: {req_err}")
        except Exception as e:
            import traceback
            print(f"\nAn error occurred during script execution: {e}")
            print("Traceback:")
            traceback.print_exc()