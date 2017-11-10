from estimize.data import *
from estimize.data.beta import BetaCalculator


def main():
    start_date = '2015-01-01'
    end_date = '2016-01-05'
    assets = get_assets(['AAPL', 'MSFT', 'AMZN'])

    beta_calculator = BetaCalculator(
        start_date=start_date,
        end_date=end_date,
        assets=assets,
        window_length=252
    )
    df = beta_calculator.call()
    print df


if __name__ == "__main__":
    main()
