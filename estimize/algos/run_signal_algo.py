from estimize.algos.signal_algo import *

def main():
    run_algorithm(
        start=pd.to_datetime('2015-01-01').tz_localize('America/New_York'),
        end=pd.to_datetime('2015-12-31').tz_localize('America/New_York'),
        initialize=initialize,
        capital_base=1e6,
        before_trading_start=before_trading_start
    )


if __name__ == "__main__":
    main()