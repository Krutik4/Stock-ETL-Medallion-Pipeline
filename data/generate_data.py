import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import random
import uuid

# Constants
COMPANIES = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "GOOGL": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.",
    "META": "Meta Platforms Inc.",
    "NVDA": "NVIDIA Corporation",
    "TSLA": "Tesla Inc.",
    "JPM": "JPMorgan Chase & Co.",
    "WMT": "Walmart Inc.",
    "PG": "Procter & Gamble Company",
}

INITIAL_PRICES = {
    "AAPL": 175.0,
    "MSFT": 330.0,
    "GOOGL": 140.0,
    "AMZN": 145.0,
    "META": 300.0,
    "NVDA": 485.0,
    "TSLA": 250.0,
    "JPM": 148.0,
    "WMT": 162.0,
    "PG": 153.0,
}

NUM_USERS = 50
START_DATE = datetime(2024, 10, 1)
END_DATE = datetime(2024, 12, 31)
MARKET_HOURS = range(9, 17)  # 9 AM to 4 PM


def is_trading_day(date):
    """Check if given date is a trading day (Monday-Friday, excluding major holidays)"""
    if date.weekday() >= 5:  # Saturday or Sunday
        return False

    holidays = [
        datetime(2024, 9, 2),  # Labor Day
        datetime(2024, 11, 28),  # Thanksgiving
        datetime(2024, 12, 25),  # Christmas
    ]
    return date.date() not in [holiday.date() for holiday in holidays]


def generate_price_movement():
    """Generate a small random price movement"""
    return np.random.normal(
        0, 0.002
    )  # Small standard deviation for realistic movements


def generate_stock_data():
    """Generate hourly stock price data"""
    data = []
    current_prices = INITIAL_PRICES.copy()

    current_date = START_DATE
    while current_date <= END_DATE:
        if is_trading_day(current_date):
            for hour in MARKET_HOURS:
                timestamp = current_date.replace(hour=hour)

                # Update prices with small random movements
                for ticker in COMPANIES.keys():
                    movement = generate_price_movement()
                    current_prices[ticker] *= 1 + movement
                    data.append(
                        {
                            "timestamp": timestamp,
                            "ticker": ticker,
                            "price": round(current_prices[ticker], 2),
                        }
                    )

        current_date += timedelta(days=1)

    return pd.DataFrame(data)


def generate_market_index(stock_data):
    """Generate market index based on stock prices"""
    index_data = []
    grouped = stock_data.groupby("timestamp")

    for timestamp, group in grouped:
        index_value = group["price"].mean() * 10  # Simple average-based index
        index_data.append(
            {"timestamp": timestamp, "index_value": round(index_value, 2)}
        )

    return pd.DataFrame(index_data)


def generate_transactions(stock_data):
    """Generate user transactions"""
    transactions = []
    user_ids = [f"USER_{i:03d}" for i in range(NUM_USERS)]

    grouped = stock_data.groupby("timestamp")

    for timestamp, group in grouped:
        # Generate 5-15 orders per hour
        num_orders = random.randint(5, 15)

        for _ in range(num_orders):
            transaction_id = str(uuid.uuid4())
            user_id = random.choice(user_ids)

            # Generate 1-3 stock transactions per order
            num_stocks = random.randint(1, 3)
            stocks_subset = random.sample(list(COMPANIES.keys()), num_stocks)

            order_transactions = []
            for ticker in stocks_subset:
                stock_price = group[group["ticker"] == ticker]["price"].iloc[0]
                shares = random.randint(10, 100) * 10  # Multiple of 10 shares

                # Ensure buy/sell balance
                if random.random() < 0.5:
                    action = "buy"
                else:
                    action = "sell"

                order_transactions.append(
                    {
                        "ticker": ticker,
                        "action": action,
                        "shares": shares,
                        "price": stock_price,
                    }
                )

            transactions.append(
                {
                    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "transaction_id": transaction_id,
                    "user_id": user_id,
                    "transactions": order_transactions,
                }
            )

    return transactions


def main():
    # Create company info CSV
    company_df = pd.DataFrame(
        [{"ticker": ticker, "company_name": name} for ticker, name in COMPANIES.items()]
    )
    company_df.to_csv("company_info.csv", index=False)

    # Generate stock price data
    stock_data = generate_stock_data()
    stock_data.to_csv("stock_prices.csv", index=False)

    # Generate market index
    market_index = generate_market_index(stock_data)
    market_index.to_csv("market_index.csv", index=False)

    # Generate transactions
    transactions = generate_transactions(stock_data)
    with open("transactions.json", "w") as f:
        json.dump(transactions, f, indent=2)


if __name__ == "__main__":
    main()
