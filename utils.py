
def parse_inputs(getXlsx, get10k, getProxyStatement, getBalanceSheet,
                 getIncomeStatement, getCashFlowStatement, years):

    raw_files_to_send = {
        "xlsx": getXlsx,
        "10k": get10k,
        "proxy_statement": getProxyStatement
    }
    merged_files_to_send = {
        "balance_sheet": getBalanceSheet,
        "income": getIncomeStatement,
        "cash": getCashFlowStatement,
    }
    start_year, end_year = years.split("-")
    years = [str(year) for year in range(int(start_year),
                                         int(end_year) + 1)]

    return raw_files_to_send, merged_files_to_send, years
