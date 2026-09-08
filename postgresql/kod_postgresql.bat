@echo off
REM SKRYPT - PRACA MAGISTERSKA
REM BAZA DANYCH: PostgreSQL 15 (Architektura Primary-Replica)
REM NARZEDZIE: YCSB 0.17.0

REM Automatyczne przejście do katalogu YCSB względem lokalizacji tego skryptu
cd /d "%~dp0..\ycsb-0.17.0"

REM TEST 1: BASELINE (Normalna praca, Replikacja asynchroniczna, 100 000 operacji)

echo ROZPOCZECIE TESTU 1: BASELINE...
bin\ycsb.bat run jdbc -P workloads\workloada -p db.driver=org.postgresql.Driver -p db.url=jdbc:postgresql://localhost:5432/ycsb -p db.user=postgres -p db.passwd=postgres -p operationcount=100000


REM KONFIGURACJA: Przełączenie klastra w tryb replikacji synchronicznej
echo WLACZANIE REPLIKACJI SYNCHRONICZNEJ...
docker exec -it postgres-primary psql -U postgres -c "ALTER SYSTEM SET synchronous_standby_names = '*';"
docker exec -it postgres-primary psql -U postgres -c "SELECT pg_reload_conf();"


REM TEST 2: SCENARIUSZ 1 i 4 - Podział sieciowy i odzyskiwanie (10 000 operacji)
echo ODLACZANIE REPLIK OD SIECI (SYMULACJA AWARII SIECI)...
docker network disconnect projekt_default postgres-replica1
docker network disconnect projekt_default postgres-replica2

echo ROZPOCZECIE TESTU 2
bin\ycsb.bat run jdbc -P workloads\workloada -p db.driver=org.postgresql.Driver -p db.url=jdbc:postgresql://localhost:5432/ycsb -p db.user=postgres -p db.passwd=postgres -p operationcount=10000

echo PRZYWRACANIE POLACZENIA SIECIOWEGO I RESTART REPLIK...
docker network connect projekt_default postgres-replica1
docker network connect projekt_default postgres-replica2
docker restart postgres-replica1
docker restart postgres-replica2


REM TEST 3: SCENARIUSZ 3 - Symulacja opóźnień sieciowych (5 000 operacji)
docker exec -it -u 0 postgres-primary bash -c "apt-get update && apt-get install -y iproute2"

echo WPROWADZANIE OPOZNIENIA 50ms NA INTERFEJS GLOWNY...
docker exec -it --privileged -u 0 postgres-primary tc qdisc add dev eth0 root netem delay 50ms

echo ROZPOCZECIE TESTU 3...
bin\ycsb.bat run jdbc -P workloads\workloada -p db.driver=org.postgresql.Driver -p db.url=jdbc:postgresql://localhost:5432/ycsb -p db.user=postgres -p db.passwd=postgres -p operationcount=5000

echo USUWANIE OPOZNIENIA SIECIOWEGO...
docker exec -it --privileged -u 0 postgres-primary tc qdisc del dev eth0 root


REM TEST 4: SCENARIUSZ 2 - Awaria węzła nadrzędnego (10 000 operacji)
echo ROZPOCZECIE TESTU 4 (W TRAKCIE TESTU WEZEL GLOWNY ZOSTANIE ZATRZYMANY)...
REM Uwaga: Komendę docker stop wykonano w osobnym oknie podczas trwania testu:
REM docker stop postgres-primary

bin\ycsb.bat run jdbc -P workloads\workloada -p db.driver=org.postgresql.Driver -p db.url=jdbc:postgresql://localhost:5432/ycsb -p db.user=postgres -p db.passwd=postgres -p operationcount=10000

echo PROCEDURA FAILOVER - AWANSOWANIE REPLIKI 1 NA NOWEGO LIDERA...
docker exec -it postgres-replica1 psql -U postgres -c "SELECT pg_promote();"
docker exec -it postgres-replica1 psql -U postgres -c "SELECT pg_is_in_recovery();"

echo BADANIA POSTGRESQL ZAKONCZONE.
pause