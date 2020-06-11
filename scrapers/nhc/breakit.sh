# future year
python3 nhc.py --storms_to_get='' --odds_container='testcontainer' --year='2025' --scrapetype='latest'
# inexistent storm 
python3 nhc.py --storms_to_get='bar' --odds_container='testcontainer' --year='2019' --scrapetype='latest'
# inexisting scrapetype
python3 nhc.py --storms_to_get='barry' --odds_container='testcontainer' --year='2019' --scrapetype='last'
# container that doesnt exist
python3 nhc.py --storms_to_get='barry' --odds_container='tetainer' --year='2019' --scrapetype='latest'
# nonexistent year
python3 nhc.py --storms_to_get='' --odds_container='testcontainer' --year='2005' --scrapetype='latest'






