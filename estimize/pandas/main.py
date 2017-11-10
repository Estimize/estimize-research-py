import ondemand

from pandas_datareader import data
from pandas_datareader.yahoo.quotes import _yahoo_codes

_yahoo_codes.update({'Market Cap': 'j1'})
_yahoo_codes.update({'Earnings/Share': 'e'})


def main():
    test_pandas()


def test_barchart():
    od = ondemand.OnDemandClient(api_key='4d817b4c400cfa2f170edd6886cc62ef')
    results = od.financial_highlights ('AAPL')['results']
    print results


def test_pandas():
    df = data.DataReader('AAPL/PRICES', 'quandl', "2015-01-01", "2015-01-05")

    print df


if __name__ == "__main__":
    main()
