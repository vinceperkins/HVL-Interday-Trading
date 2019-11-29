
from pylivetrader.api import (
    attach_pipeline,
    date_rules,
    time_rules,
    order,
    order_target_percent,
    get_open_orders,
    cancel_order,
    pipeline_output,
    schedule_function,
)

import numpy as np
import pandas as pd
#Testing
from pipeline_live.engine import LivePipelineEngine
from pipeline_live.data.sources.iex import list_symbols
#
#API imports for pipeline
from pylivetrader.finance.execution import LimitOrder
from zipline.pipeline import Pipeline, CustomFactor
from pipeline_live.data.alpaca.pricing import USEquityPricing
#from pipeline_live.data.iex.pricing import USEquityPricing
from pipeline_live.data.polygon.filters import (
    IsPrimaryShareEmulation as IsPrimaryShare)
from pipeline_live.data.iex.factors import RSI, Returns
#
#
#
class AnnualizedVolatility(CustomFactor):
    inputs = [Returns(window_length=2)]
    params = {'annualization_factor': 10.0}
    window_length = 10
    def compute(self, today, assets, out, returns, annualization_factor):
        out[:] = np.nanstd(returns, axis=0) * (annualization_factor ** .5)

import logbook
log = logbook.Logger('algo')

def record(*args, **kwargs):
    print('args={}, kwargs={}'.format(args, kwargs))
#
#
#
def initialize (context): # runs once when script starts
    log.info("Welcome Vincent Perkins")
    #context is a python dictionary that contains information on portfolio/performance.
    context.idr_losers = pd.Series(([])) #intraday losing stocks
    context.day_count = 0
    context.daily_message = "Day {}."
    context.open_orders = get_open_orders()
    context.backup_stocks = symbols('BAM')
    context.age = {} #empty dictionary maps one value to another
    #context.usep = USEquityPricing() #USEquityPricing object

    #Factor criteria
    close_price = USEquityPricing.close.latest
    vol = USEquityPricing.volume.latest
    ann_var = AnnualizedVolatility()
    rsi = RSI()

    #screening
    mask_custom = (IsPrimaryShare() & (vol < 150000) & (close_price > 1) & (close_price < 3) & (ann_var > 0.815) & (rsi < 50))
    stockBasket = USEquityPricing.close.latest.top(3000,  mask = mask_custom)
    
    #Column construction
    pipe_columns = {'close_price': close_price, 'volume': vol, 'ann_var': ann_var}
    
    #Creation of actual pipeline
    pipe = Pipeline(columns = pipe_columns, screen = stockBasket)
    attach_pipeline(pipe, 'Stocks')
    #Testing
    log.info(USEquityPricing.get_loader())
    eng = LivePipelineEngine(list_symbols)
    df = eng.run_pipeline(pipe)
    log.info(df)

    #Schedule functions
    schedule_function(day_start, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 1))
    schedule_function(late_day_trade, date_rules.every_day(), time_rules.market_open(hours = 5, minutes = 56)) #offset open tells when to run a user defined function
    schedule_function(check_portfolio, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 1))
    schedule_function(morning_day_trade1, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 15))
    schedule_function(morning_day_trade2, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 45))
    schedule_function(check_portfolio, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 48))
    schedule_function(cancel_open_orders, date_rules.every_day(),time_rules.market_close(hours=0, minutes=1))
    
def day_start(context, data):
    context.day_count += 1
    log.info(context.daily_message, context.day_count)
    log.info("Good Morning")
    for stock in context.portfolio.positions:
        if (stock in context.age):
            context.age[stock] += 1
            #log.info(context.age[stock])
        else:
            context.age[stock] = 1
        for stock in context.age:
            if stock not in context.portfolio.positions:
                context.age[stock] = 0
                
def late_day_trade(context, data):
    #Get the pipeline output
    pipe_output = pipeline_output('Stocks')
    context.days_stocks = pipe_output.sort_values(by =['ann_var'], ascending = False)
    log.info(context.daily_message, context.day_count)
    log.info(context.days_stocks)
    
    #Calculate Daily Return Top Losers
    if (context.days_stocks.size > 0):
        price_history = data.history(context.days_stocks.index, "price", 745, "1m") #356 +390
        open_prices = price_history.iloc[0]
        current_prices = price_history.iloc[-1]
        context.idr_losers = ((current_prices - open_prices) / open_prices).sort_values()
        context.idr_losers = context.idr_losers[0:7]#5
        context.idr_losers = context.idr_losers[context.idr_losers < -0.0]      
        log.info(context.idr_losers)
    else:
        price_history = data.history(context.backup_stocks, "price", 1 , "1m") #356
        current_prices = price_history.iloc[-1]
        context.idr_losers = current_prices #Stock info is irrelevant here  
          
    pct_cash = context.portfolio.cash/context.portfolio.portfolio_value
    
    #Get Open Orders and Buy
    cmd_string = []
    for stock in context.idr_losers.index:
        if(data.can_trade(stock)):
            if(stock not in context.open_orders):
                order_target_percent(stock, pct_cash/(context.idr_losers.size + 1))
                q = math.floor(cash_portion/data.current(stock, 'price'))
                side = "buy"
                cmd_string.extend([stock.symbol, q, side])
                    
    #Check Portfolio
    log.info(cmd_string)
    record(leverage = context.account.leverage) #be sure to always track leverage
    record(cash = context.portfolio.cash)
    record(port_value = context.portfolio.portfolio_value)
   
def morning_day_trade1(context, data):
    log.info(context.daily_message, context.day_count)
    for stock in context.portfolio.positions:
        if((data.current(stock, 'price')) - context.portfolio.positions[stock].cost_basis)/context.portfolio.positions[stock].cost_basis > 0.001:
            if (context.portfolio.positions[stock].cost_basis > 0):
                num_shares = context.portfolio.positions[stock].amount
                order_target(stock, num_shares/2)
                #log.info("{} Current Price = {} :: Cost Basis = {}",stock.symbol, data.current(stock, 'price'), context.portfolio.positions[stock].cost_basis)
                       
def morning_day_trade2(context, data):
    for stock in context.portfolio.positions:
        if((data.current(stock, 'price')) - context.portfolio.positions[stock].cost_basis)/context.portfolio.positions[stock].cost_basis > 0.001:
            if ((context.portfolio.positions[stock].amount > 0) or (data.current(stock, 'price') < 0.30)):
                order_target_percent(stock, 0)
                #log.info("{} Current Price = {} :: Cost Basis = {}",stock.symbol, data.current(stock, 'price'), context.portfolio.positions[stock].cost_basis)
                
                
def check_portfolio(context, data): #Check for possible splits
    i = 0
    for stock in context.portfolio.positions:
        i = i + 1
        if ((data.current(stock, 'price') - context.portfolio.positions[stock].cost_basis)/(context.portfolio.positions[stock].cost_basis) > 0.1):
            log.info("*** {} Current Price = {} :: Cost Basis = {} :: Amount = {} :: Age = {} ",stock.symbol, data.current(stock, 'price'), context.portfolio.positions[stock].cost_basis, context.portfolio.positions[stock].amount, context.age[stock])
    log.info("Portfolio Size: {}", i)
    
def cancel_open_orders(context, data):
    oo = get_open_orders()
    if len(oo) == 0:
        return
    for stock, orders in oo.items():
        for o in orders:
            log.info("Canceling order of {} shares in {}", o.amount, stock)
            cancel_order(o)