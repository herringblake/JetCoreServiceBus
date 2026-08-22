# Goal is to create a simple service bus. 

Built in Python

Kafka queues (unless we find something better)
  * Explore NATS JetStream as an alternative

Should be able to run from `docker compose up`

Kafka will be an "open" event bus, meaning any adapter that is allowed to connect and communicate will be able to see all messages as they move across

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


Native support for data tokenization

Open ended event structure
* All messages will have a baseline schema to keep in order
* Payload will be defined as generic object
* All message schemas will have a type. 
  * It's up to the listener to know the data coming at it.
  * All payload schemas should be documented and available for reference