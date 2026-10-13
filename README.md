# Aigle utils

This repo regroup every scripts that are external to the app repos [aigle-api](https://github.com/MTES-MCT/aigle-api) and [aigle-frontend](https://github.com/MTES-MCT/aigle-frontend)

## SQL Scripts

There is a pre-commit hook that can be used by installing locally `pgFormatter`:
```
# debian / ubuntu
sudo apt-get install pgformatter

# macOs
brew install pgformatter
```

You can run `pg_format` manually on all sql scripts by running:
```
find sql_scripts -name "*.sql" -exec pg_format -i {} \;
```

## Python scripts

1. Create a `venv`:
```
python3 -m venv venv
source venv/bin/activate
```

2. Install packages:
```
pip install -r ./python_scripts/requirements.txt
```
3. Run the script you want to use:
```
python csv_to_geopackage.py your_file.csv output.gpkg
```
