from faker import Faker
import random
from datetime import datetime, timedelta

import csv

class FakeDataGenerator:
    def __init__(self, existing_data):
        self.fake = Faker()
        self.existing_data = existing_data

    def generate_fake_row(self, num_products):
        customer_row = random.choice(self.existing_data)
        self.existing_data.sort(key=lambda x: x['Order Date'], reverse=True)

        print(f"inside generate_fake_row : {len(self.existing_data)}")


        name_initials = ''.join([x[0].upper() for x in customer_row["Customer Name"].split(' ')]) + "-"
        order_id_suffix = "".join([str(random.randint(0, 9)) for _ in range(5)])
        order_id = name_initials + order_id_suffix
        order_date = self.fake.date_between_dates(datetime(2022, 1, 1), datetime.now())
        ship_date = order_date + timedelta(days=random.randint(1, 10))
        ship_mode = self.fake.random_element(["Standard", "Express"])
        record_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fake_rows = []
        for _ in range(num_products):
            product_row = random.choice(self.existing_data)
            sales = round(random.uniform(200, 500), 2)
            quantity = random.randint(1, 10)
            discount = round(random.uniform(0, 0.2), 2)
            profit = round(random.uniform(-20, 20), 2)

            fake_row = {
                "Order ID": order_id,
                "Record Date": record_date,
                "Order Date": order_date,
                "Ship Date": ship_date,
                "Ship Mode": ship_mode,
                "Customer ID": customer_row["Customer ID"],
                "Customer Name": customer_row["Customer Name"],
                "Segment": customer_row["Segment"],
                "Country": customer_row["Country"],
                "City": customer_row["City"],
                "State": customer_row["State"],
                "Postal Code": self.fake.postcode(),
                "Region": self.fake.random_element(["North", "South", "East", "West"]),
                "Product ID": product_row["Product ID"],
                "Category": product_row["Category"],
                "Sub-Category": product_row["Sub-Category"],
                "Product Name": product_row["Product Name"],
                "Sales": sales,
                "Quantity": quantity,
                "Discount": discount,
                "Profit": profit,
            }
            fake_rows.append(fake_row)

        return fake_rows

    def generate_fake_data(self):
        num_rows = random.randint(100, 400)  # Random number of rows between 5 and 20
        fake_data = []
        for _ in range(num_rows):
            num_products = random.randint(1, 5)
            fake_row = self.generate_fake_row(num_products)
            fake_data.extend(fake_row)

        return fake_data

