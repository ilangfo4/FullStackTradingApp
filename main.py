from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
import sqlite3, config 
from datetime import date

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
def index(request: Request):
    stock_filter = request.query_params.get('filter', False)
    
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row

    cursor = connection.cursor()

    if stock_filter == 'new_closing_highs':
        cursor.execute("""
                select * from(
                    select symbol, company, stock_id, max(close), date
                    from stock_price join stock on stock.id = stock_price.stock_id
                    group by stock_id
                    order by symbol
                ) where date = ?       
                       """, (date.today().isoformat(),))
    else:   
        cursor.execute("""
        SELECT id, symbol, company FROM stock ORDER BY symbol          
    """)
 
    
    rows = cursor.fetchall()
    
    return templates.TemplateResponse("index.html", {"request" : request, "stocks" : rows})
    
@app.get("/stock/{symbol}")
def stock_detail(request: Request, symbol):
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row

    cursor = connection.cursor()

    cursor.execute("""
                SELECT * FROM strategy    
                   """)
    strategies = cursor.fetchall()
    
    cursor.execute("""
        SELECT id, symbol, company FROM stock WHERE symbol = ?         
    """, (symbol,))

    row = cursor.fetchone()
    
    cursor.execute("""
        SELECT * FROM stock_price WHERE stock_id = ? ORDER BY date DESC     
    """, (row['id'],))
     
    prices = cursor.fetchall()
    return templates.TemplateResponse("stock_detail.html", {"request" : request, "stock" : row, "bars" : prices, "strategies" : strategies})

@app.post("/apply_strategy")
def apply_strategy(strategy_id: int = Form(...), stock_id: int = Form(...)):
    connection = sqlite3.connect(config.DB_FILE)
    cursor = connection.cursor()
    
    cursor.execute("""
                INSERT INTO stock_strategy(stock_id, strategy_id) VALUES (?, ?)    
                    """, (stock_id, strategy_id,))
     
    connection.commit()
    
    return  RedirectResponse(url=f"/strategy/{strategy_id}", status_code = 303)

@app.get("/strategy/{strategy_id}")
def strategy(request: Request, strategy_id: int):
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    
    cursor = connection.cursor()
    
    cursor.execute("""
                SELECT id, name
                FROM strategy
                WHERE id = ?     
                   """, (strategy_id,))
    strategy = cursor.fetchone()
    
    cursor.execute("""
                SELECT symbol, company
                FROM stock JOIN stock_strategy on stock_strategy.stock_id = stock.id
                WHERE strategy_id = ?
                   
                   """, (strategy_id,))
    
    stocks = cursor.fetchall()
    
    return templates.TemplateResponse("strategy.html" , {"request" : request, "stocks" : stocks, "strategy" : strategy})