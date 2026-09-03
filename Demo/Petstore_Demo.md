# Petstore Demo

## Petstore Service
* Implements Petstore example swagger (./Demo/resources/openapi.yaml)
* Uses database connector for persistence
* Detects a pet invoice file (CSV) triggering
  + for each pet -> POST: /pet
  + add entry to ACH file for payment
* Create a list of pet suppliers
  + add supplier APIs if needed and update openAPI document
  + include pet types supplied
  + supplier company info
  + payment (bank RTN & account)
* use Moov ACH service
* Setup Postman collection for Petstore and any additional APIs required.
* Any workflows should be diagramed using mermaid
* Any required data mappings/transformations should be documented with LinkML
