# Development Step Guidelines

This is a list of standards for generating code and associated documentation:

Project code will be well documented and organized
* projects will have a git repository
* project code will be formatted in accordence with accepted standards for its environment
* project code will be dockerized
  * a docker-compose file will be gererated for the entier project
  * document environment properties
* project will be organized in accordence to their environment 
  * example: filetree for a project would have expected files and folders
  * environment configs are correct and content is formatted to expected norms
* list all necessary dependencies (frameworks, libraries, services, SDKs, etc...)
  * descibe what feature/objects/functions are needed and how they will be used
  * note 
    * version
    * release date
    * source
  * describe what security validations have been performed on outside resources
    * note how future security validations may be performed
* document dev environment configuration
  * programming environment
    * language
    * compiler/runtime
    * version(s)
    * environment properties
      * system paths
      * local paths
        * example: Python virtual environment
  * development tools used
  