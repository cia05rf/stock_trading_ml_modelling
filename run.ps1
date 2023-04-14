#This batch file will run the stock trading scrape and output found buy signals

#Activate the venv
C:\Users\cia05\AppData\Local\pypoetry\Cache\virtualenvs\stock-trading-ml-modelling-nHRQ5qjo-py3.9\Scripts\activate.ps1
#Cd into propper drive
Set-Location C:\Users\cia05\OneDrive\Documents\GitHub\stock_trading_ml_modelling
#Run the python script
python -m stock_trading_ml_modelling.functions
#Exit
Exit