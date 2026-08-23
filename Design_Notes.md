# 1.) Design a simple service bus. 

Intro:
We are going to perform a design exercise. The goal will be to come up with a design and an implementation plan. At this stage, we should not be generating any code. We will take short steps and discuss all componenets, what exists, what needs to be built. If something needs to be built, we will discuss how and what are best libraries to utilize.

Messages will be carried by NATS JetStream

NATS JetStream will be used as an "open" event bus, meaning any adapter that is allowed to connect and communicate will be able to see all messages as they move across. (meta data will be present but payload will be encrypted and only accessible to those services registered)

Adapters will be written for collecting and moving information across the bus
* HTTP adapter will be emit and receive
* adapters will be configured to listen to specific messages
* adapters will have built in encryption of PII
  
Fully encrypted bus
* Access will use public key encryption
  * services will use a public key for access similar to SSH
  * app publisher will share public key with "bus"
  * bus will allow access based on valid signature
* Payload data will be encrypted before transmission
  * payload data will be decrypted after
  * encrypted data will be encrypted for all public keys allowed to receive data
    * public key encryption allows for a message to be encrypted against multiple public keys(recipients).


Open ended event structure
* All messages will have a baseline schema to keep in order
* Payload will be defined as generic object
* All message schemas will have a subject. 
  * It's up to the listener to know the data coming at it based on subject.
  * All subjects will be a unique value
  * All payload schemas should be documented and available for reference

Events
Need to develop a baseline for event metadata but this is a high level example:
_note: for the example below, all functions are assumed to be pseudocode_
```
  "event": {
    "eventDetails": {
      "eventId": uuid7(),
      "eventCreated": timestamp(),
      "eventType": "SOMEEVENTTYPE"
    },
    "eventPayload": encoded_string
  }
```

Initial Adapters:
* HTTP request/response (REST service consumer)
* Database adapter (MySQL to start)
* Webhook sender
* Webhook listener
* REST API Service
* File Storage (local)

Project will be coded in Python
* show how project will be organized
  * should be like a typical Python project
* list all necessary Python libaries
  * descibe what objects/functions are needed and how they will be used
* list all necessary services
* list required development tools
  * testing
  * security scanning
  * docker
  * etc..

Finished product will include a working `docker-compose.yml`
