# Server to download 10k Files from the SEC website

This server's goal is to gather data following the principles in Value Investing classic book [Good Stock Cheap](https://www.amazon.com/Good-Stocks-Cheap-Confidence-Outperformance/dp/125983607X) by Kenneth Jeffrey Marshall.

Run 
```
uvicorn app:app --host 0.0.0.0 --port 8080 --reload
```
to start the server

And then query the server with a request like
```
http://0.0.0.0:8080/params/?ticker=AAPL&years=2018-2022&_10k=true&Proxy=true&Balance=true&Income=true&Cash=true
```

Description of the parameters:
- ticker: the ticker corresponding to the company to analyze
- years: the range of years to pull data of (in format `start_year-end_year`)
- _10k: boolean parameter to return the 10K PDF file (format: `true` or `false`)
- Proxy: boolean parameter to return the Proxy Statement PDF file (format: `true` or `false`)
- Balance: boolean parameter to return the Balance Sheet as Excel file (format: `true` or `false`)
- Income: boolean parameter to return the Income Statement as Excel file (format: `true` or `false`)
- Cash: boolean parameter to return the Cash Flow Statement as Excel file (format: `true` or `false`)
