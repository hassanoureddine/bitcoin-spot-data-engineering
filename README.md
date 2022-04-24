# bitcoin-spot-data-engineering

## Development
Tasks: 
- Collect bitcoin spot historical data source
- Use Python to interact and ingest the data source. Stored initially as CSV files.
- Setup an account with BigQuery
- Update Python script to load BigQuery tables 

### Prerequisite
* Get a free API key from [CoinAPI.io](https://www.coinapi.io/)
* Setup [BigQuery](https://cloud.google.com/bigquery) and create Service Account
        
### Setup google-cloud for Python 
```
pip install --upgrade google-cloud
pip install --upgrade google-cloud-bigquery
pip install --upgrade google-cloud-storage
```
