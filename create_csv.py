import pandas as pd
import random
import uuid
from faker import Faker
import numpy as np

# Initialize Faker instance
fake = Faker()

# Set the number of rows you want in your CSV file
number_of_rows = 100000

# Define categories and products
categories = ['Home', 'Toys', 'Clothing', 'Books', 'Electronics', 'Sports']
products = {
    'Home': ['Lamp', 'Chair', 'Table', 'Curtains', 'Sofa'],
    'Toys': ['Action Figure', 'Doll', 'Puzzle', 'Lego Set', 'Toy Car'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Hat', 'Socks'],
    'Books': ['Novel', 'Biography', 'Science Fiction', 'Self-Help', 'Cookbook'],
    'Electronics': ['Smartphone', 'Tablet', 'Laptop', 'Headphones', 'Camera'],
    'Sports': ['Football', 'Tennis Racket', 'Basketball', 'Yoga Mat', 'Dumbbells'],
}

# Create data lists
data = {
    'order_id': [],
    'customer_id': [],
    'order_date': [],
    'product_id': [],
    'product_name': [],
    'category': [],
    'quantity': [],
    'price_per_unit': [],
    'total_price': [],
    'payment_method': [],
    'shipping_address': [],
    'status': []
}

# Define payment methods and statuses
payment_methods = ['Credit Card', 'Bank Transfer', 'Cash', 'Paypal']
statuses = ['Delivered', 'Shipped', 'Pending', 'Cancelled']

# Generate bad synthetic data
for _ in range(number_of_rows):
    # 20% chance of creating a missing or invalid value in each field
    order_id = str(uuid.uuid4()) if random.random() > 0.2 else None
    customer_id = str(uuid.uuid4()) if random.random() > 0.2 else "INVALID_CUSTOMER_ID" if random.random() > 0.9 else None
    order_date = fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S") if random.random() > 0.1 else "InvalidDate"
    product_id = random.randint(1000, 1100) if random.random() > 0.1 else "BadProductID"
    category = random.choice(categories) if random.random() > 0.15 else "UnknownCategory"
    product_name = random.choice(products[category]) if category in products else None
    quantity = random.randint(1, 10) if random.random() > 0.15 else -5  # Include negative quantities
    price_per_unit = round(random.uniform(5, 1000), 2) if random.random() > 0.1 else "N/A"  # Include non-numeric values
    total_price = round(price_per_unit * quantity, 2) if isinstance(price_per_unit, (int, float)) and quantity > 0 else None
    payment_method = random.choice(payment_methods) if random.random() > 0.15 else "CashOnDelivery"
    shipping_address = fake.address().replace("\n", ", ") if random.random() > 0.1 else None
    status = random.choice(statuses) if random.random() > 0.1 else "UnknownStatus"

    # Append the generated data
    data['order_id'].append(order_id)
    data['customer_id'].append(customer_id)
    data['order_date'].append(order_date)
    data['product_id'].append(product_id)
    data['product_name'].append(product_name)
    data['category'].append(category)
    data['quantity'].append(quantity)
    data['price_per_unit'].append(price_per_unit)
    data['total_price'].append(total_price)
    data['payment_method'].append(payment_method)
    data['shipping_address'].append(shipping_address)
    data['status'].append(status)

# Create DataFrame and export to CSV
df = pd.DataFrame(data)
df.to_csv('bad_ecommerce_sales_data.csv', index=False)

print("CSV file with 10,000 rows of bad data generated successfully!")
