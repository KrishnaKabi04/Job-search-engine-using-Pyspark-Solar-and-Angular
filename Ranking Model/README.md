#README

###One-time process
1. Open terminal/ command prompt
2. Go into 'solr-8.10.1' folder
3. Run the following commands:
 	Solr Start : ./bin/solr.cmd start
    Solr create collection command: ./bin/solr.cmd create -c tweets -s 2 -rf 2
    Solr create collection command: ./bin/solr.cmd create -c users -s 2 -rf 2
    (If the machine is NOT a windows machine, kindly remove ".cmd" and execute the above)
4. Import the project as a Maven project
5. Run SolrSetup.java
6. Run SolrIndexer.java

###Recuring Process (To fire up the server)
1. Run SpringBootStarter.java through Maven Build commands
   mvn spring-boot:run
