docker run \
  --name neo4j-container \
  -d \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/12345678 neo4j

# localhost:7474
# CREATE DATABASE dataset100;
# CREATE DATABASE dataset75;
# CREATE DATABASE dataset50;
# CREATE DATABASE dataset25;
