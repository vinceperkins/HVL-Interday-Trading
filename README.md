# HVL-Interday-Trading
Conversion of Quantopian to pylivetrader algorithm. 
This algorithm uses the zipline library to generate a basket of stocks that follow certain criteria. The stocks with the highest annualized volatility and lowest daily percentage change  in the basket are bought at the end of the current trading day and sold during the next trading day.
