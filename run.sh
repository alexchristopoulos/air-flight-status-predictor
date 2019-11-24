cd /home/admin/air-flight-status-predictor
rm -rvf target
mvn package
clear
echo -e "*** ${RED} EXECUTING JAR FILE ${NC} ***"
java -jar ./target/air-flight-status-predictor-1.0-SNAPSHOT-jar-with-dependencies.jar