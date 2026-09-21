"""Fills the Postgres source database with fake users and orders.

The clickstream producer covers the streaming side of the pipeline; this script
covers the batch side by writing history straight into the OLTP tables that dbt
reads from. Run it once after `docker compose up`.

Usage:
    python ingestion/generate_orders.py --users 2000 --orders 20000 --days 180
"""

import argparse
import os
import random
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv()

COUNTRIES = ["US", "GB", "CA", "DE", "FR", "IN", "AU", "JP", "BR"]
CHANNELS = ["organic", "paid_search", "social", "email", "referral", "direct"]
SEGMENTS = ["new", "returning", "vip", "at_risk"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer"]
ORDER_STATUSES = ["delivered", "shipped", "confirmed", "cancelled", "returned"]
STATUS_WEIGHTS = [0.60, 0.15, 0.13, 0.07, 0.05]


def connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )


def make_users(count):
    """Build a list of user rows. User ids match the clickstream producer."""
    users = []
    for i in range(1, count + 1):
        user_id = "USR-%04d" % i
        users.append((
            user_id,
            "user%d@example.com" % i,
            random.choice(COUNTRIES),
            random.choice(CHANNELS),
            random.choice(SEGMENTS),
            random.random() < 0.8,
            datetime.now() - timedelta(days=random.randint(0, 720)),
        ))
    return users


def make_orders(num_orders, num_users, product_ids, days):
    """Build order and order_item rows spread over the last `days` days."""
    orders = []
    items = []

    for i in range(1, num_orders + 1):
        order_id = "ORD-%06d" % i
        ordered_at = datetime.now() - timedelta(
            days=random.randint(0, days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        # One to four different products per order.
        chosen = random.sample(product_ids, random.randint(1, 4))
        total = 0.0
        for j, (product_id, base_price) in enumerate(chosen, start=1):
            quantity = random.randint(1, 3)
            # Sale price wobbles a bit around the catalogue price.
            unit_price = round(float(base_price) * random.uniform(0.8, 1.0), 2)
            total += quantity * unit_price
            items.append(("%s-%d" % (order_id, j), order_id, product_id, quantity, unit_price))

        total = round(total, 2)
        discount = round(total * random.choice([0.0, 0.0, 0.05, 0.10, 0.20]), 2)

        orders.append((
            order_id,
            "USR-%04d" % random.randint(1, num_users),
            random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0],
            total,
            discount,
            random.choice(PAYMENT_METHODS),
            random.choice(COUNTRIES),
            ordered_at,
        ))

    return orders, items


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=2000)
    parser.add_argument("--orders", type=int, default=20000)
    parser.add_argument("--days", type=int, default=180, help="spread orders over this many days")
    args = parser.parse_args()

    conn = connect()
    cur = conn.cursor()

    cur.execute("SELECT product_id, base_price FROM products")
    product_ids = cur.fetchall()
    if not product_ids:
        raise SystemExit("No products found. Did init.sql run?")

    print("Inserting %d users..." % args.users)
    cur.executemany(
        """
        INSERT INTO users (user_id, email, country, acquisition_channel, segment,
                           email_verified, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        make_users(args.users),
    )

    print("Inserting %d orders..." % args.orders)
    orders, items = make_orders(args.orders, args.users, product_ids, args.days)
    cur.executemany(
        """
        INSERT INTO orders (order_id, user_id, status, total_amount, discount_amount,
                            payment_method, shipping_country, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
        """,
        orders,
    )

    print("Inserting %d order items..." % len(items))
    cur.executemany(
        """
        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (order_item_id) DO NOTHING
        """,
        items,
    )

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
