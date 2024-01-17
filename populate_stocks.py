import sqlite3, config
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT symbol, company FROM stock           
""")

rows = cursor.fetchall()
symbols = [row['symbol'] for row in rows]

#creating client info
trading_client = TradingClient(config.API_KEY, config.API_SKEY, paper = True)
account = trading_client.get_account()

#searching for assets
search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)

assets = trading_client.get_all_assets(search_params)

for asset in assets:
    try:
        if asset.status == 'active' and asset.tradable and asset.symbol not in symbols: 
            print(f"Added a new stock  {asset.symbol} {asset.name}")
            cursor.execute("INSERT INTO stock (symbol, company, exchange) VALUES (?, ?, ?)" , (asset.symbol, asset.name, asset.exchange))
    except Exception as e:
        print(asset.symbol)
        print(e)
        
    
connection.commit()
