#!/usr/bin/env python3
"""
Gelson's Market Mock Data Generator

Generates realistic retail data for an organic grocery store chain.
Includes intentional data quality issues for ETL pipeline testing.

Usage:
    python generate_gelsons_data.py [--output-dir PATH] [--seed SEED]
"""

import csv
import json
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import string

# Try to import Faker, fall back to basic generation if not available
try:
    from faker import Faker
    fake = Faker()
    Faker.seed(42)
    HAS_FAKER = True
except ImportError:
    HAS_FAKER = False
    print("Warning: Faker not installed. Using basic data generation.")

# Seed for reproducibility
random.seed(42)

# Constants
OUTPUT_DIR = Path("/Users/pankajsharma/Documents/Development/thedatastudio/data/bronze/gelsons")

# Store locations (real Gelson's-inspired LA area locations)
STORE_LOCATIONS = [
    {"city": "Pacific Palisades", "zip": "90272", "address": "15424 Sunset Blvd"},
    {"city": "Santa Monica", "zip": "90401", "address": "2627 Wilshire Blvd"},
    {"city": "Century City", "zip": "90067", "address": "10250 Santa Monica Blvd"},
    {"city": "Silver Lake", "zip": "90039", "address": "2725 Hyperion Ave"},
    {"city": "Calabasas", "zip": "91302", "address": "22277 Mulholland Hwy"},
]

# Product categories and subcategories for organic grocery
CATEGORIES = {
    "Produce": ["Fruits", "Vegetables", "Herbs", "Salads"],
    "Dairy": ["Milk", "Cheese", "Yogurt", "Eggs", "Butter"],
    "Meat & Seafood": ["Beef", "Poultry", "Pork", "Fish", "Shellfish"],
    "Bakery": ["Bread", "Pastries", "Cakes", "Cookies"],
    "Deli": ["Prepared Foods", "Sandwiches", "Salads", "Soups"],
    "Frozen": ["Ice Cream", "Frozen Meals", "Frozen Vegetables", "Frozen Fruits"],
    "Beverages": ["Juice", "Water", "Coffee", "Tea", "Kombucha"],
    "Pantry": ["Pasta", "Rice", "Canned Goods", "Oils", "Spices", "Snacks"],
    "Health & Beauty": ["Vitamins", "Skincare", "Haircare", "Supplements"],
    "Household": ["Cleaning", "Paper Products", "Pet Supplies"],
}

# Organic brands
BRANDS = [
    "Gelson's Finest", "Organic Valley", "Amy's", "Nature's Path", "Annie's",
    "Earthbound Farm", "Stonyfield", "Horizon Organic", "365 by Whole Foods",
    "Newman's Own", "Clif Bar", "Kashi", "Garden of Eatin'", "Late July",
    "Applegate", "Niman Ranch", "Mary's Gone Crackers", "GT's Kombucha",
    None, None, None  # Some products have no brand (store brand)
]

# Dietary preferences
DIETARY_PREFERENCES = [
    "vegan", "vegetarian", "gluten-free", "dairy-free", "keto",
    "paleo", "organic-only", "low-sodium", "nut-free", "kosher"
]

# Payment methods
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "Apple Pay", "Google Pay", "Gift Card"]

# Departments for employees
DEPARTMENTS = ["Produce", "Dairy", "Meat & Seafood", "Bakery", "Deli", "Front End", "Grocery", "Management"]

# Positions
POSITIONS = {
    "Front End": ["Cashier", "Bagger", "Customer Service Rep"],
    "Produce": ["Produce Clerk", "Produce Lead"],
    "Dairy": ["Dairy Clerk"],
    "Meat & Seafood": ["Butcher", "Seafood Specialist"],
    "Bakery": ["Baker", "Bakery Clerk"],
    "Deli": ["Deli Clerk", "Prep Cook"],
    "Grocery": ["Stock Clerk", "Inventory Specialist"],
    "Management": ["Store Manager", "Assistant Manager", "Department Manager"],
}

# Supplier certifications
CERTIFICATIONS = [
    "USDA Organic", "Non-GMO Project Verified", "Certified Humane",
    "Fair Trade", "Rainforest Alliance", "Animal Welfare Approved",
    "Certified B Corporation", "USDA Organic, Non-GMO Project"
]


def generate_id(prefix: str, num: int, width: int = 5) -> str:
    """Generate a formatted ID string."""
    return f"{prefix}-{str(num).zfill(width)}"


def random_date(start_year: int = 2020, end_year: int = 2024) -> str:
    """Generate a random date string."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    date = start + timedelta(days=random_days)
    return date.strftime("%Y-%m-%d")


def random_datetime(start_date: datetime, end_date: datetime) -> str:
    """Generate a random datetime string."""
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    dt = start_date + timedelta(seconds=random_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def random_phone() -> str:
    """Generate a random phone number."""
    area_codes = ["310", "323", "213", "818", "626", "562", "714", "949"]
    return f"({random.choice(area_codes)}) {random.randint(100,999)}-{random.randint(1000,9999)}"


def random_email(first_name: str, last_name: str, domain: str = None) -> str:
    """Generate an email address."""
    if domain is None:
        domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com"]
        domain = random.choice(domains)

    formats = [
        f"{first_name.lower()}.{last_name.lower()}@{domain}",
        f"{first_name.lower()}{last_name.lower()}@{domain}",
        f"{first_name[0].lower()}{last_name.lower()}@{domain}",
        f"{first_name.lower()}{random.randint(1,99)}@{domain}",
    ]
    return random.choice(formats)


def random_name() -> tuple:
    """Generate a random first and last name."""
    if HAS_FAKER:
        return fake.first_name(), fake.last_name()

    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
                   "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
                   "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Nancy", "Matthew", "Lisa"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
                  "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"]
    return random.choice(first_names), random.choice(last_names)


def random_company_name() -> str:
    """Generate a random company/farm name."""
    if HAS_FAKER:
        return fake.company()

    prefixes = ["Valley", "Sunny", "Green", "Golden", "Pacific", "Mountain", "Ocean", "River"]
    suffixes = ["Farms", "Organic", "Naturals", "Foods", "Produce", "Dairy", "Ranch"]
    return f"{random.choice(prefixes)} {random.choice(suffixes)}"


def generate_product_description(name: str, category: str, is_organic: bool) -> str:
    """Generate a product description for RAG processing."""
    organic_text = "organic, " if is_organic else ""
    descriptions = [
        f"Premium {organic_text}{name.lower()} sourced from local California farms. Perfect for health-conscious shoppers.",
        f"Fresh, {organic_text}high-quality {name.lower()}. Sustainably grown and harvested at peak ripeness.",
        f"Our {organic_text}{name.lower()} is carefully selected for exceptional taste and nutrition. Great for everyday use.",
        f"Delicious {organic_text}{name.lower()} from trusted suppliers. No artificial additives or preservatives.",
        f"Hand-picked {organic_text}{name.lower()} with superior flavor. A customer favorite in our {category} department.",
    ]
    return random.choice(descriptions)


def generate_supplier_description(company_name: str, certification: str) -> str:
    """Generate a supplier description for RAG processing."""
    years = random.randint(10, 50)
    descriptions = [
        f"{company_name} has been providing premium organic products for over {years} years. {certification} certified.",
        f"Family-owned since {2024-years}, {company_name} specializes in sustainable farming practices. Holds {certification} certification.",
        f"A trusted partner in organic sourcing, {company_name} delivers quality products with {certification} standards.",
        f"{company_name} operates sustainable farms in California, committed to {certification} practices for {years} years.",
    ]
    return random.choice(descriptions)


def introduce_data_quality_issues(records: List[Dict], config: Dict) -> List[Dict]:
    """Introduce intentional data quality issues into records."""
    result = records.copy()

    # Null values
    if "null_fields" in config:
        for field, pct in config["null_fields"].items():
            for record in result:
                if field in record and random.random() < pct:
                    record[field] = None

    # Duplicates
    if "duplicate_pct" in config and config["duplicate_pct"] > 0:
        num_dupes = int(len(result) * config["duplicate_pct"])
        for _ in range(num_dupes):
            dupe = random.choice(result).copy()
            # Slightly modify the duplicate
            if "id" in dupe:
                dupe["id"] = generate_id("DUP", random.randint(10000, 99999))
            result.append(dupe)

    # Invalid formats
    if "invalid_email_pct" in config:
        for record in result:
            if "email" in record and record["email"] and random.random() < config["invalid_email_pct"]:
                # Create invalid email
                invalid_formats = [
                    record["email"].replace("@", ""),
                    record["email"].replace(".", ""),
                    "invalid-email",
                    "@nodomain.com",
                ]
                record["email"] = random.choice(invalid_formats)

    # Negative values (anomalies)
    if "negative_fields" in config:
        for field, pct in config["negative_fields"].items():
            for record in result:
                if field in record and record[field] and random.random() < pct:
                    record[field] = -abs(float(record[field]))

    # Inconsistent date formats
    if "inconsistent_date_pct" in config:
        for record in result:
            for key, value in record.items():
                if "date" in key.lower() and value and random.random() < config["inconsistent_date_pct"]:
                    # Convert to different format
                    try:
                        if " " in str(value):
                            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                            formats = ["%m/%d/%Y %I:%M %p", "%d-%m-%Y %H:%M", "%Y/%m/%d"]
                        else:
                            dt = datetime.strptime(value, "%Y-%m-%d")
                            formats = ["%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d", "%B %d, %Y"]
                        record[key] = dt.strftime(random.choice(formats))
                    except:
                        pass

    return result


def generate_stores() -> List[Dict]:
    """Generate store data."""
    stores = []
    for i, loc in enumerate(STORE_LOCATIONS, 1):
        stores.append({
            "store_id": generate_id("GEL-STR", i, 3),
            "store_name": f"Gelson's {loc['city']}",
            "address": loc["address"],
            "city": loc["city"],
            "state": "CA",
            "zip_code": loc["zip"],
            "phone": random_phone(),
            "manager_id": generate_id("GEL-EMP", i, 3),  # Will be updated after employees
            "square_footage": random.randint(25000, 45000),
            "opened_date": random_date(1990, 2015),
            "store_type": "Full Service" if i <= 3 else random.choice(["Full Service", "Express"]),
        })
    return stores


def generate_suppliers(count: int = 50) -> List[Dict]:
    """Generate supplier data."""
    suppliers = []
    ca_cities = ["Oxnard", "Salinas", "Fresno", "Bakersfield", "Modesto", "Ventura",
                 "Santa Maria", "Watsonville", "Gilroy", "Hollister"]

    for i in range(1, count + 1):
        first, last = random_name()
        company = random_company_name()
        cert = random.choice(CERTIFICATIONS)

        suppliers.append({
            "supplier_id": generate_id("SUP", i, 3),
            "company_name": company,
            "contact_name": f"{first} {last}",
            "email": random_email(first, last, f"{company.lower().replace(' ', '')}.com"),
            "phone": random_phone(),
            "address": f"{random.randint(100, 9999)} {random.choice(['Farm', 'Ranch', 'Organic', 'Valley'])} Rd",
            "city": random.choice(ca_cities),
            "state": "CA",
            "zip_code": str(random.randint(93000, 95999)),
            "certification": cert,
            "lead_time_days": random.randint(1, 7),
            "minimum_order": round(random.uniform(200, 2000), 2),
            "description": generate_supplier_description(company, cert),
        })

    return suppliers


def generate_products(count: int = 500) -> List[Dict]:
    """Generate product catalog."""
    products = []

    # Product name templates by category
    product_templates = {
        "Produce": {
            "Fruits": ["Organic Fuji Apples", "Organic Gala Apples", "Organic Bananas", "Strawberries", "Blueberries",
                      "Organic Avocados", "Lemons", "Limes", "Oranges", "Grapes", "Mangoes", "Peaches", "Raspberries",
                      "Blackberries", "Cherries", "Organic Pears", "Watermelon", "Cantaloupe", "Honeydew", "Kiwi"],
            "Vegetables": ["Organic Kale", "Baby Spinach", "Organic Carrots", "Broccoli", "Cauliflower",
                          "Organic Tomatoes", "Bell Peppers", "Zucchini", "Cucumbers", "Organic Lettuce"],
            "Herbs": ["Fresh Basil", "Organic Cilantro", "Rosemary", "Thyme", "Mint", "Parsley"],
            "Salads": ["Mixed Greens", "Spring Mix", "Caesar Salad Kit", "Kale Salad Mix"],
        },
        "Dairy": {
            "Milk": ["Organic Whole Milk", "Oat Milk", "Almond Milk", "Organic 2% Milk", "Coconut Milk"],
            "Cheese": ["Organic Cheddar", "Goat Cheese", "Brie", "Parmesan", "Mozzarella", "Gruyere"],
            "Yogurt": ["Greek Yogurt", "Organic Vanilla Yogurt", "Coconut Yogurt", "Skyr"],
            "Eggs": ["Organic Free-Range Eggs", "Pasture-Raised Eggs", "Cage-Free Eggs"],
            "Butter": ["Organic Butter", "Grass-Fed Butter", "European Style Butter"],
        },
        "Meat & Seafood": {
            "Beef": ["Grass-Fed Ground Beef", "Organic Ribeye", "Filet Mignon", "NY Strip"],
            "Poultry": ["Organic Chicken Breast", "Free-Range Whole Chicken", "Ground Turkey"],
            "Pork": ["Heritage Pork Chops", "Bacon", "Organic Ham"],
            "Fish": ["Wild Salmon", "Ahi Tuna", "Halibut", "Cod", "Sea Bass"],
            "Shellfish": ["Shrimp", "Scallops", "Lobster Tail", "Crab"],
        },
        "Bakery": {
            "Bread": ["Sourdough Loaf", "Whole Wheat Bread", "Multigrain Bread", "Baguette"],
            "Pastries": ["Croissant", "Pain au Chocolat", "Danish", "Scone"],
            "Cakes": ["Chocolate Cake", "Carrot Cake", "Cheesecake"],
            "Cookies": ["Chocolate Chip Cookies", "Oatmeal Raisin", "Macarons"],
        },
        "Deli": {
            "Prepared Foods": ["Rotisserie Chicken", "Meatloaf", "Mac and Cheese"],
            "Sandwiches": ["Turkey Club", "Veggie Wrap", "Chicken Caesar Wrap"],
            "Salads": ["Quinoa Salad", "Greek Salad", "Pasta Salad"],
            "Soups": ["Chicken Noodle Soup", "Tomato Bisque", "Minestrone"],
        },
        "Frozen": {
            "Ice Cream": ["Organic Vanilla", "Chocolate Gelato", "Coconut Ice Cream"],
            "Frozen Meals": ["Organic Burrito", "Veggie Lasagna", "Chicken Tikka Masala"],
            "Frozen Vegetables": ["Organic Peas", "Mixed Vegetables", "Edamame"],
            "Frozen Fruits": ["Frozen Berries", "Mango Chunks", "Acai Packs"],
        },
        "Beverages": {
            "Juice": ["Fresh Orange Juice", "Green Juice", "Apple Juice"],
            "Water": ["Sparkling Water", "Mineral Water", "Coconut Water"],
            "Coffee": ["Organic Coffee Beans", "Cold Brew", "Espresso Roast"],
            "Tea": ["Green Tea", "Chamomile", "Earl Grey"],
            "Kombucha": ["Ginger Kombucha", "Berry Kombucha", "Original Kombucha"],
        },
        "Pantry": {
            "Pasta": ["Organic Spaghetti", "Penne", "Gluten-Free Pasta"],
            "Rice": ["Brown Rice", "Jasmine Rice", "Quinoa"],
            "Canned Goods": ["Organic Tomatoes", "Black Beans", "Chickpeas"],
            "Oils": ["Olive Oil", "Coconut Oil", "Avocado Oil"],
            "Spices": ["Himalayan Salt", "Black Pepper", "Turmeric", "Cinnamon"],
            "Snacks": ["Organic Chips", "Trail Mix", "Granola Bars", "Nuts"],
        },
        "Health & Beauty": {
            "Vitamins": ["Multivitamin", "Vitamin D", "Vitamin C", "B Complex"],
            "Skincare": ["Organic Lotion", "Face Cream", "Sunscreen"],
            "Haircare": ["Organic Shampoo", "Conditioner", "Hair Oil"],
            "Supplements": ["Probiotics", "Fish Oil", "Collagen"],
        },
        "Household": {
            "Cleaning": ["All-Purpose Cleaner", "Dish Soap", "Laundry Detergent"],
            "Paper Products": ["Paper Towels", "Toilet Paper", "Tissues"],
            "Pet Supplies": ["Organic Dog Food", "Cat Food", "Pet Treats"],
        },
    }

    product_id = 1
    for category, subcats in CATEGORIES.items():
        # Generate products for each subcategory
        templates = product_templates.get(category, {})
        for subcat in subcats:
            names = templates.get(subcat, [f"{subcat} Item {i}" for i in range(1, 6)])
            for name in names:
                if product_id > count:
                    break

                is_organic = random.random() < 0.7  # 70% organic
                is_local = random.random() < 0.4  # 40% local

                # Price ranges by category
                price_ranges = {
                    "Produce": (1.99, 9.99),
                    "Dairy": (2.99, 12.99),
                    "Meat & Seafood": (8.99, 39.99),
                    "Bakery": (3.99, 24.99),
                    "Deli": (6.99, 19.99),
                    "Frozen": (4.99, 14.99),
                    "Beverages": (1.99, 8.99),
                    "Pantry": (2.99, 15.99),
                    "Health & Beauty": (5.99, 29.99),
                    "Household": (3.99, 19.99),
                }

                min_price, max_price = price_ranges.get(category, (2.99, 15.99))
                unit_price = round(random.uniform(min_price, max_price), 2)

                # Unit types
                unit_types = {
                    "Produce": ["lb", "each", "bunch", "oz"],
                    "Dairy": ["each", "oz", "qt", "gal"],
                    "Meat & Seafood": ["lb", "oz", "each"],
                    "default": ["each", "pack", "oz"],
                }
                unit_type = random.choice(unit_types.get(category, unit_types["default"]))

                # Nutritional info as JSON
                nutritional_info = json.dumps({
                    "calories": random.randint(20, 500),
                    "protein": random.randint(0, 30),
                    "carbs": random.randint(0, 50),
                    "fat": random.randint(0, 25),
                    "fiber": random.randint(0, 10),
                })

                products.append({
                    "product_id": generate_id("GEL-PRD", product_id),
                    "name": name,
                    "category": category,
                    "subcategory": subcat,
                    "brand": random.choice(BRANDS),
                    "unit_price": unit_price,
                    "unit_type": unit_type,
                    "is_organic": is_organic,
                    "is_local": is_local,
                    "description": generate_product_description(name, category, is_organic),
                    "nutritional_info": nutritional_info,
                    "shelf_life_days": random.choice([3, 5, 7, 14, 30, 60, 90, 180, 365]),
                    "created_at": random_datetime(datetime(2022, 1, 1), datetime(2024, 6, 1)),
                })
                product_id += 1

    # Apply data quality issues
    products = introduce_data_quality_issues(products, {
        "null_fields": {"brand": 0.05},
        "duplicate_pct": 0.02,
        "negative_fields": {"unit_price": 0.01},
        "inconsistent_date_pct": 0.03,
    })

    return products


def generate_employees(stores: List[Dict], count: int = 200) -> List[Dict]:
    """Generate employee data."""
    employees = []
    store_ids = [s["store_id"] for s in stores]

    for i in range(1, count + 1):
        first, last = random_name()
        dept = random.choice(DEPARTMENTS)
        positions = POSITIONS.get(dept, ["Associate"])

        employees.append({
            "employee_id": generate_id("GEL-EMP", i, 3),
            "first_name": first,
            "last_name": last,
            "email": random_email(first, last, "gelsons.com"),
            "phone": random_phone(),
            "store_id": random.choice(store_ids),
            "department": dept,
            "position": random.choice(positions),
            "hire_date": random_date(2015, 2024),
            "hourly_rate": round(random.uniform(16.50, 45.00), 2),
            "is_active": random.random() < 0.9,  # 90% active
        })

    # Apply data quality issues
    employees = introduce_data_quality_issues(employees, {
        "null_fields": {"hire_date": 0.02, "phone": 0.05},
        "duplicate_pct": 0.01,
    })

    return employees


def generate_customers(stores: List[Dict], count: int = 2000) -> List[Dict]:
    """Generate customer/loyalty member data."""
    customers = []
    store_ids = [s["store_id"] for s in stores]

    for i in range(1, count + 1):
        first, last = random_name()

        # Random dietary preferences (0-3)
        prefs = random.sample(DIETARY_PREFERENCES, k=random.randint(0, 3))

        customers.append({
            "customer_id": generate_id("GEL-CUS", i),
            "first_name": first,
            "last_name": last,
            "email": random_email(first, last),
            "phone": random_phone(),
            "join_date": random_date(2018, 2024),
            "membership_tier": random.choices(
                ["Bronze", "Silver", "Gold", "Platinum"],
                weights=[40, 30, 20, 10]
            )[0],
            "preferred_store_id": random.choice(store_ids),
            "dietary_preferences": json.dumps(prefs) if prefs else None,
            "total_lifetime_spend": round(random.uniform(100, 15000), 2),
            "is_active": random.random() < 0.85,
        })

    # Apply data quality issues
    customers = introduce_data_quality_issues(customers, {
        "null_fields": {"phone": 0.08, "dietary_preferences": 0.15},
        "duplicate_pct": 0.02,
        "invalid_email_pct": 0.03,
        "negative_fields": {"total_lifetime_spend": 0.01},
    })

    return customers


def generate_product_suppliers(products: List[Dict], suppliers: List[Dict]) -> List[Dict]:
    """Generate product-supplier relationships."""
    relationships = []
    product_ids = [p["product_id"] for p in products if p.get("product_id")]
    supplier_ids = [s["supplier_id"] for s in suppliers]

    for product_id in product_ids:
        # Each product has 1-3 suppliers
        num_suppliers = random.randint(1, 3)
        selected_suppliers = random.sample(supplier_ids, min(num_suppliers, len(supplier_ids)))

        for i, supplier_id in enumerate(selected_suppliers):
            relationships.append({
                "product_id": product_id,
                "supplier_id": supplier_id,
                "cost_price": round(random.uniform(0.50, 20.00), 2),
                "is_primary": i == 0,  # First supplier is primary
                "last_order_date": random_date(2023, 2024),
            })

    return relationships


def generate_inventory(products: List[Dict], stores: List[Dict]) -> List[Dict]:
    """Generate inventory data."""
    inventory = []
    product_ids = [p["product_id"] for p in products if p.get("product_id")]
    store_ids = [s["store_id"] for s in stores]

    inv_id = 1
    for store_id in store_ids:
        # Not all products in all stores
        store_products = random.sample(product_ids, int(len(product_ids) * 0.8))

        for product_id in store_products:
            # Find product for shelf life
            product = next((p for p in products if p.get("product_id") == product_id), None)
            shelf_life = product.get("shelf_life_days", 30) if product else 30

            # Calculate expiry based on shelf life
            today = datetime.now()
            expiry = today + timedelta(days=random.randint(1, shelf_life))

            inventory.append({
                "inventory_id": generate_id("INV", inv_id),
                "product_id": product_id,
                "store_id": store_id,
                "quantity_on_hand": random.randint(0, 200),
                "quantity_reserved": random.randint(0, 20),
                "reorder_point": random.randint(10, 50),
                "reorder_quantity": random.randint(50, 200),
                "last_restocked": random_date(2024, 2024),
                "expiry_date": expiry.strftime("%Y-%m-%d") if shelf_life < 60 else None,
                "storage_location": f"{random.choice(string.ascii_uppercase)}-{random.randint(1,20)}-{random.randint(1,5)}",
            })
            inv_id += 1

    # Apply data quality issues
    inventory = introduce_data_quality_issues(inventory, {
        "null_fields": {"expiry_date": 0.1},
        "negative_fields": {"quantity_on_hand": 0.02},
    })

    return inventory


def generate_transactions(
    customers: List[Dict],
    stores: List[Dict],
    employees: List[Dict],
    count: int = 5000
) -> List[Dict]:
    """Generate transaction data."""
    transactions = []

    customer_ids = [c["customer_id"] for c in customers if c.get("customer_id")]
    store_ids = [s["store_id"] for s in stores]
    cashier_ids = [e["employee_id"] for e in employees
                   if e.get("department") == "Front End" and e.get("is_active")]

    if not cashier_ids:
        cashier_ids = [e["employee_id"] for e in employees[:20]]

    # Generate transactions over the last 12 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    for i in range(1, count + 1):
        # Weighted time distribution (peak hours)
        hour = random.choices(
            list(range(7, 22)),  # 7am to 9pm
            weights=[2, 3, 4, 8, 8, 10, 10, 8, 6, 5, 8, 10, 10, 8, 5]
        )[0]

        # Random date with weekend weighting
        days_ago = random.randint(0, 365)
        txn_date = end_date - timedelta(days=days_ago)

        # 30% more likely on weekends
        if txn_date.weekday() >= 5:
            if random.random() > 0.7:
                continue  # Skip some weekday transactions

        txn_date = txn_date.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )

        # Calculate totals
        subtotal = round(random.uniform(15, 250), 2)
        tax_rate = 0.0925  # CA sales tax
        tax_amount = round(subtotal * tax_rate, 2)
        discount = round(random.uniform(0, subtotal * 0.15), 2) if random.random() < 0.3 else 0
        total = round(subtotal + tax_amount - discount, 2)

        # 15% are guest checkouts (no customer_id)
        customer_id = random.choice(customer_ids) if random.random() > 0.15 else None

        transactions.append({
            "transaction_id": f"TXN-{txn_date.strftime('%Y%m%d')}-{str(i).zfill(5)}",
            "customer_id": customer_id,
            "store_id": random.choice(store_ids),
            "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "subtotal": subtotal,
            "tax_amount": tax_amount,
            "discount_amount": discount,
            "total_amount": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "cashier_id": random.choice(cashier_ids),
        })

    # Apply data quality issues
    transactions = introduce_data_quality_issues(transactions, {
        "negative_fields": {"total_amount": 0.01},
        "inconsistent_date_pct": 0.02,
    })

    return transactions


def generate_transaction_items(
    transactions: List[Dict],
    products: List[Dict]
) -> List[Dict]:
    """Generate transaction line items."""
    items = []

    product_list = [p for p in products if p.get("product_id") and p.get("unit_price")]
    if not product_list:
        return items

    item_id = 1
    for txn in transactions:
        if not txn.get("transaction_id"):
            continue

        # 3-15 items per transaction
        num_items = random.randint(3, 15)
        txn_products = random.sample(product_list, min(num_items, len(product_list)))

        for product in txn_products:
            quantity = round(random.uniform(1, 5), 1) if random.random() < 0.2 else random.randint(1, 5)
            unit_price = abs(float(product["unit_price"])) if product["unit_price"] else 5.99

            # Occasional price variation from catalog
            if random.random() < 0.1:
                unit_price = round(unit_price * random.uniform(0.9, 1.1), 2)

            line_total = round(quantity * unit_price, 2)
            discount = round(line_total * random.uniform(0, 0.2), 2) if random.random() < 0.15 else 0

            items.append({
                "item_id": generate_id("ITM", item_id, 8),
                "transaction_id": txn["transaction_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
                "discount_applied": discount,
                "is_weighted": random.random() < 0.2,
            })
            item_id += 1

    # Add some orphaned items (referential integrity issues)
    for i in range(int(len(items) * 0.01)):
        orphan = random.choice(items).copy()
        orphan["item_id"] = generate_id("ITM", item_id + i, 8)
        orphan["transaction_id"] = f"TXN-ORPHAN-{random.randint(10000, 99999)}"
        items.append(orphan)

    # Apply data quality issues
    items = introduce_data_quality_issues(items, {
        "negative_fields": {"quantity": 0.005},
    })

    return items


def write_csv(data: List[Dict], filepath: Path):
    """Write data to CSV file."""
    if not data:
        print(f"Warning: No data to write to {filepath}")
        return

    # Get all unique keys
    keys = []
    for record in data:
        for key in record.keys():
            if key not in keys:
                keys.append(key)

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

    print(f"Created: {filepath} ({len(data)} records)")


def main():
    parser = argparse.ArgumentParser(description="Generate Gelson's Market mock data")
    parser.add_argument("--output-dir", type=str, default=str(OUTPUT_DIR),
                        help="Output directory for CSV files")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    args = parser.parse_args()

    # Set seed
    random.seed(args.seed)
    if HAS_FAKER:
        Faker.seed(args.seed)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Gelson's Market Data Generator")
    print("=" * 60)
    print(f"Output directory: {output_dir}")
    print(f"Random seed: {args.seed}")
    print()

    # Generate data in dependency order
    print("Generating data...")

    print("  1. Stores...")
    stores = generate_stores()

    print("  2. Suppliers...")
    suppliers = generate_suppliers(50)

    print("  3. Products...")
    products = generate_products(500)

    print("  4. Product Suppliers...")
    product_suppliers = generate_product_suppliers(products, suppliers)

    print("  5. Employees...")
    employees = generate_employees(stores, 200)

    # Update store managers
    for store in stores:
        manager = next((e for e in employees
                       if e.get("store_id") == store["store_id"]
                       and e.get("position") in ["Store Manager", "Manager"]), None)
        if manager:
            store["manager_id"] = manager["employee_id"]

    print("  6. Customers...")
    customers = generate_customers(stores, 2000)

    print("  7. Inventory...")
    inventory = generate_inventory(products, stores)

    print("  8. Transactions...")
    transactions = generate_transactions(customers, stores, employees, 5000)

    print("  9. Transaction Items...")
    transaction_items = generate_transaction_items(transactions, products)

    print()
    print("Writing CSV files...")

    # Write all files
    write_csv(stores, output_dir / "stores.csv")
    write_csv(suppliers, output_dir / "suppliers.csv")
    write_csv(products, output_dir / "products.csv")
    write_csv(product_suppliers, output_dir / "product_suppliers.csv")
    write_csv(employees, output_dir / "employees.csv")
    write_csv(customers, output_dir / "customers.csv")
    write_csv(inventory, output_dir / "inventory.csv")
    write_csv(transactions, output_dir / "transactions.csv")
    write_csv(transaction_items, output_dir / "transaction_items.csv")

    print()
    print("=" * 60)
    print("Generation complete!")
    print("=" * 60)

    # Summary
    print("\nDataset Summary:")
    print(f"  Stores:            {len(stores):,}")
    print(f"  Suppliers:         {len(suppliers):,}")
    print(f"  Products:          {len(products):,}")
    print(f"  Product Suppliers: {len(product_suppliers):,}")
    print(f"  Employees:         {len(employees):,}")
    print(f"  Customers:         {len(customers):,}")
    print(f"  Inventory:         {len(inventory):,}")
    print(f"  Transactions:      {len(transactions):,}")
    print(f"  Transaction Items: {len(transaction_items):,}")
    print(f"\n  Total Records:     {sum([len(stores), len(suppliers), len(products), len(product_suppliers), len(employees), len(customers), len(inventory), len(transactions), len(transaction_items)]):,}")


if __name__ == "__main__":
    main()
