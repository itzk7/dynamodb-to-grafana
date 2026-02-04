"""
Data Generation Script for E-commerce POC
Generates realistic customer, product, and order data for DynamoDB tables
"""
import boto3
import random
import string
from datetime import datetime, timedelta
from decimal import Decimal
import argparse
import time
from faker import Faker

# Initialize Faker for generating realistic data
fake = Faker()

# Initialize boto3 clients
dynamodb = boto3.resource('dynamodb')

# Product categories and sample data
PRODUCT_CATEGORIES = [
    'Electronics', 'Clothing', 'Home & Garden', 
    'Sports & Outdoors', 'Books', 'Toys & Games',
    'Health & Beauty', 'Food & Beverage'
]

PRODUCT_NAMES = {
    'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart Watch', 'Camera', 'Speaker', 'Monitor'],
    'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Sneakers', 'Sweater', 'Shorts', 'Hat'],
    'Home & Garden': ['Lamp', 'Vase', 'Pillow', 'Rug', 'Mirror', 'Plant Pot', 'Curtains', 'Clock'],
    'Sports & Outdoors': ['Yoga Mat', 'Dumbbell', 'Bicycle', 'Tent', 'Backpack', 'Water Bottle', 'Running Shoes'],
    'Books': ['Novel', 'Cookbook', 'Biography', 'Self-Help', 'Mystery', 'Science Fiction', 'History'],
    'Toys & Games': ['Board Game', 'Puzzle', 'Action Figure', 'Doll', 'Building Blocks', 'Card Game'],
    'Health & Beauty': ['Shampoo', 'Moisturizer', 'Perfume', 'Lipstick', 'Sunscreen', 'Face Mask'],
    'Food & Beverage': ['Coffee', 'Tea', 'Chocolate', 'Snack Bar', 'Juice', 'Cookies', 'Nuts']
}

REGIONS = ['US-East', 'US-West', 'EU-North', 'EU-South', 'Asia-Pacific']

ORDER_STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']


def generate_id(prefix: str, length: int = 8) -> str:
    """Generate a random ID with prefix"""
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"{prefix}-{random_part}"


def generate_customers(table_name: str, count: int = 1000) -> list:
    """Generate customer records"""
    print(f"Generating {count} customers...")
    
    table = dynamodb.Table(table_name)
    customers = []
    
    for i in range(count):
        customer = {
            'CustomerID': generate_id('CUST', 10),
            'Name': fake.name(),
            'Email': fake.email(),
            'Region': random.choice(REGIONS),
            'JoinDate': (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
            'Phone': fake.phone_number(),
            'Address': fake.address().replace('\n', ', ')
        }
        customers.append(customer)
        
        # Batch write in groups of 25 (DynamoDB limit)
        if len(customers) == 25:
            with table.batch_writer() as batch:
                for cust in customers:
                    batch.put_item(Item=cust)
            print(f"  Written {i+1}/{count} customers...")
            customers = []
            time.sleep(0.1)  # Rate limiting
    
    # Write remaining customers
    if customers:
        with table.batch_writer() as batch:
            for cust in customers:
                batch.put_item(Item=cust)
    
    print(f"✓ Generated {count} customers")
    return customers


def generate_products(table_name: str, count: int = 100) -> list:
    """Generate product records"""
    print(f"Generating {count} products...")
    
    table = dynamodb.Table(table_name)
    products = []
    
    for i in range(count):
        category = random.choice(PRODUCT_CATEGORIES)
        base_name = random.choice(PRODUCT_NAMES[category])
        
        product = {
            'ProductID': generate_id('PROD', 8),
            'Name': f"{base_name} {fake.color_name()} {fake.random_element(['Pro', 'Plus', 'Classic', 'Premium', 'Basic'])}",
            'Category': category,
            'Price': Decimal(str(round(random.uniform(9.99, 999.99), 2))),
            'StockLevel': random.randint(0, 500),
            'Description': fake.text(max_nb_chars=100),
            'SKU': ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        }
        products.append(product)
        
        # Batch write in groups of 25
        if len(products) == 25:
            with table.batch_writer() as batch:
                for prod in products:
                    batch.put_item(Item=prod)
            print(f"  Written {i+1}/{count} products...")
            products = []
            time.sleep(0.1)
    
    # Write remaining products
    if products:
        with table.batch_writer() as batch:
            for prod in products:
                batch.put_item(Item=prod)
    
    print(f"✓ Generated {count} products")
    return products


def generate_orders(orders_table: str, customers_table: str, products_table: str, count: int = 5000) -> list:
    """Generate order records"""
    print(f"Generating {count} orders...")
    
    # Fetch customer and product IDs
    print("  Fetching customer IDs...")
    customers_tbl = dynamodb.Table(customers_table)
    scan_response = customers_tbl.scan(ProjectionExpression='CustomerID')
    customer_ids = [item['CustomerID'] for item in scan_response['Items']]
    
    # Handle pagination if needed
    while 'LastEvaluatedKey' in scan_response:
        scan_response = customers_tbl.scan(
            ProjectionExpression='CustomerID',
            ExclusiveStartKey=scan_response['LastEvaluatedKey']
        )
        customer_ids.extend([item['CustomerID'] for item in scan_response['Items']])
    
    print("  Fetching product IDs...")
    products_tbl = dynamodb.Table(products_table)
    scan_response = products_tbl.scan(ProjectionExpression='ProductID,Price')
    products = [(item['ProductID'], item['Price']) for item in scan_response['Items']]
    
    # Handle pagination if needed
    while 'LastEvaluatedKey' in scan_response:
        scan_response = products_tbl.scan(
            ProjectionExpression='ProductID,Price',
            ExclusiveStartKey=scan_response['LastEvaluatedKey']
        )
        products.extend([(item['ProductID'], item['Price']) for item in scan_response['Items']])
    
    if not customer_ids or not products:
        print("ERROR: No customers or products found. Generate customers and products first.")
        return []
    
    print(f"  Found {len(customer_ids)} customers and {len(products)} products")
    
    orders_tbl = dynamodb.Table(orders_table)
    orders = []
    
    # Generate orders with realistic patterns
    for i in range(count):
        # Create realistic order date distribution (more recent orders)
        days_ago = int(random.expovariate(1/30))  # Exponential distribution
        if days_ago > 365:
            days_ago = random.randint(0, 365)
        
        order_date = datetime.now() - timedelta(days=days_ago)
        
        # Select random customer
        customer_id = random.choice(customer_ids)
        
        # Select 1-5 products for the order
        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
        order_products = random.sample(products, min(num_items, len(products)))
        
        # Calculate total amount
        total_amount = sum(Decimal(str(price)) for _, price in order_products)
        
        # Determine order status based on date (older orders more likely to be delivered)
        if days_ago > 30:
            status = random.choices(ORDER_STATUSES, weights=[5, 5, 10, 75, 5])[0]
        elif days_ago > 7:
            status = random.choices(ORDER_STATUSES, weights=[10, 15, 30, 40, 5])[0]
        else:
            status = random.choices(ORDER_STATUSES, weights=[20, 30, 30, 15, 5])[0]
        
        order = {
            'OrderID': generate_id('ORD', 12),
            'CustomerID': customer_id,
            'OrderDate': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'Status': status,
            'TotalAmount': Decimal(str(round(total_amount, 2))),
            'ItemCount': num_items,
            'ProductIDs': [prod_id for prod_id, _ in order_products],
            'ShippingAddress': fake.address().replace('\n', ', ')
        }
        orders.append(order)
        
        # Batch write in groups of 25
        if len(orders) == 25:
            with orders_tbl.batch_writer() as batch:
                for ord in orders:
                    batch.put_item(Item=ord)
            print(f"  Written {i+1}/{count} orders...")
            orders = []
            time.sleep(0.1)
    
    # Write remaining orders
    if orders:
        with orders_tbl.batch_writer() as batch:
            for ord in orders:
                batch.put_item(Item=ord)
    
    print(f"✓ Generated {count} orders")
    return orders


def simulate_updates(orders_table: str, products_table: str, num_updates: int = 100):
    """Simulate real-time updates to existing records"""
    print(f"\nSimulating {num_updates} real-time updates...")
    
    orders_tbl = dynamodb.Table(orders_table)
    products_tbl = dynamodb.Table(products_table)
    
    # Update order statuses
    print("  Updating order statuses...")
    scan_response = orders_tbl.scan(
        FilterExpression='#status IN (:pending, :processing)',
        ExpressionAttributeNames={'#status': 'Status'},
        ExpressionAttributeValues={':pending': 'pending', ':processing': 'processing'},
        Limit=num_updates // 2
    )
    
    for item in scan_response['Items'][:num_updates // 2]:
        new_status = random.choice(['processing', 'shipped', 'delivered'])
        orders_tbl.update_item(
            Key={'OrderID': item['OrderID']},
            UpdateExpression='SET #status = :status',
            ExpressionAttributeNames={'#status': 'Status'},
            ExpressionAttributeValues={':status': new_status}
        )
        time.sleep(0.05)
    
    # Update product stock levels and prices
    print("  Updating product stock and prices...")
    scan_response = products_tbl.scan(Limit=num_updates // 2)
    
    for item in scan_response['Items'][:num_updates // 2]:
        stock_change = random.randint(-20, 50)
        new_stock = max(0, item['StockLevel'] + stock_change)
        
        # Occasionally update prices
        update_expr = 'SET StockLevel = :stock'
        expr_values = {':stock': new_stock}
        
        if random.random() < 0.3:  # 30% chance to update price
            price_change_percent = random.uniform(-0.1, 0.15)
            new_price = Decimal(str(round(float(item['Price']) * (1 + price_change_percent), 2)))
            update_expr += ', Price = :price'
            expr_values[':price'] = new_price
        
        products_tbl.update_item(
            Key={'ProductID': item['ProductID']},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values
        )
        time.sleep(0.05)
    
    print(f"✓ Completed {num_updates} updates")


def main():
    parser = argparse.ArgumentParser(description='Generate realistic e-commerce data for DynamoDB')
    parser.add_argument('--customers-table', required=True, help='DynamoDB customers table name')
    parser.add_argument('--products-table', required=True, help='DynamoDB products table name')
    parser.add_argument('--orders-table', required=True, help='DynamoDB orders table name')
    parser.add_argument('--num-customers', type=int, default=1000, help='Number of customers to generate')
    parser.add_argument('--num-products', type=int, default=100, help='Number of products to generate')
    parser.add_argument('--num-orders', type=int, default=5000, help='Number of orders to generate')
    parser.add_argument('--simulate-updates', type=int, default=0, help='Number of real-time updates to simulate')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("E-commerce Data Generator")
    print("=" * 60)
    print(f"Region: {args.region}")
    print(f"Customers Table: {args.customers_table}")
    print(f"Products Table: {args.products_table}")
    print(f"Orders Table: {args.orders_table}")
    print("=" * 60)
    print()
    
    start_time = time.time()
    
    try:
        # Generate customers
        generate_customers(args.customers_table, args.num_customers)
        
        # Generate products
        generate_products(args.products_table, args.num_products)
        
        # Generate orders
        generate_orders(args.orders_table, args.customers_table, args.products_table, args.num_orders)
        
        # Simulate real-time updates if requested
        if args.simulate_updates > 0:
            simulate_updates(args.orders_table, args.products_table, args.simulate_updates)
        
        elapsed_time = time.time() - start_time
        print()
        print("=" * 60)
        print(f"✓ Data generation complete!")
        print(f"  Total time: {elapsed_time:.2f} seconds")
        print(f"  Customers: {args.num_customers}")
        print(f"  Products: {args.num_products}")
        print(f"  Orders: {args.num_orders}")
        if args.simulate_updates > 0:
            print(f"  Updates: {args.simulate_updates}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise


if __name__ == '__main__':
    main()




