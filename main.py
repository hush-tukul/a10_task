import requests
import time
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Tuple

# Constants
BASE_URL = 'http://145.239.87.46:8000/product'
RETRY_LIMIT = 5
INITIAL_RETRY_DELAY = 1  # seconds
MAX_WORKERS = 5  # Number of parallel threads
PRODUCTS_FILE = 'products.json'  # File to save the list of products


def fetch_product(next_product_token: Optional[str] = None) -> Dict[str, Any]:
    """Fetch a product from the API with retry logic.

    Args:
        next_product_token (Optional[str]): The token for the next product to fetch.

    Returns:
        Dict[str, Any]: The JSON data of the fetched product.

    Raises:
        Exception: If the product data cannot be fetched after retries.
    """
    url = BASE_URL
    if next_product_token:
        url += f'?next_product_token={quote(next_product_token)}'

    retry_delay = INITIAL_RETRY_DELAY
    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.get(url)
            if response.status_code == 503:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff

    raise Exception(f"Failed to fetch product data for token: {next_product_token} after several retries")


def fetch_all_products_parallel() -> List[Dict[str, Any]]:
    """Fetch all products from the API using parallel requests.

    Returns:
        List[Dict[str, Any]]: A list of all fetched products.
    """
    products: List[Dict[str, Any]] = []
    next_product_token: Optional[str] = None
    start_time = time.time()  # Start the timer

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            try:
                # Fetch the first product or the next product in sequence
                product_data = fetch_product(next_product_token)
                products.append(product_data)

                elapsed_time = time.time() - start_time
                print(
                    f"[{elapsed_time:.2f}s] Added product ID {product_data['product_id']} - Total products: {len(products)}")

                next_product_token = product_data.get('next_product_token')

                # Exit loop if no more products to fetch
                if not next_product_token:
                    print(f"All products have been fetched in {elapsed_time:.2f} seconds.")
                    break

                # Fetch the next batch of products in parallel
                future_to_token = {
                    executor.submit(fetch_product, token): token
                    for token in [next_product_token]
                }

                for future in as_completed(future_to_token):
                    try:
                        result = future.result()
                        products.append(result)
                        elapsed_time = time.time() - start_time
                        print(
                            f"[{elapsed_time:.2f}s] Added product ID {result['product_id']} - Total products: {len(products)}")

                        next_product_token = result.get('next_product_token')
                        if not next_product_token:
                            print(f"All products have been fetched in {elapsed_time:.2f} seconds.")
                            break
                    except Exception as e:
                        print(f"Error occurred while processing a future: {e}")

                if not next_product_token:
                    break  # Exit loop if no more products to fetch

            except Exception as e:
                print(f"Error occurred: {e}")
                # Continue fetching next products even if the current one fails

    return products


def analyze_products(products: List[Dict[str, Any]]) -> Tuple[int, Dict[str, int], Dict[str, Any], float]:
    """Analyze the product data to generate summary statistics.

    Args:
        products (List[Dict[str, Any]]): The list of products to analyze.

    Returns:
        Tuple[int, Dict[str, int], Dict[str, Any], float]:
        - Total number of products
        - Count of products per category
        - Most expensive fashion product
        - Average price of toys & games products
    """
    total_products = len(products)
    category_counts: Dict[str, int] = defaultdict(int)
    most_expensive_fashion: Dict[str, Any] = {'price': -1}
    toys_games_total_price = 0
    toys_games_count = 0

    for product in products:
        category = product['category']
        category_counts[category] += 1

        if category == 'Fashion' and product['price'] > most_expensive_fashion['price']:
            most_expensive_fashion = {
                'id': product['product_id'],
                'name': product['product_name'],
                'price': product['price']
            }

        if category == 'Toys & Games':
            toys_games_total_price += product['price']
            toys_games_count += 1

    average_price_toys_games = (toys_games_total_price / toys_games_count) if toys_games_count else 0

    return total_products, category_counts, most_expensive_fashion, average_price_toys_games


def save_products_to_json(products: List[Dict[str, Any]], filename: str) -> None:
    """Save all products to a JSON file.

    Args:
        products (List[Dict[str, Any]]): The list of products to save.
        filename (str): The filename to save the products to.
    """
    with open(filename, 'w') as f:
        json.dump(products, f, indent=4)


def main() -> None:
    """Main function to run the script."""
    try:
        print("Fetching products...")
        products = fetch_all_products_parallel()

        # Save the fetched products to JSON file
        save_products_to_json(products, PRODUCTS_FILE)

        # Analyze the fetched products
        total_products, category_counts, most_expensive_fashion, average_price_toys_games = analyze_products(products)

        # Print analysis results
        print("\nAnalysis Results:")
        print(f"1. Total number of products: {total_products}")
        print("2. Number of products in each category:")
        for category, count in category_counts.items():
            print(f"   - {category}: {count}")
        if most_expensive_fashion['price'] >= 0:
            print(
                f"3. Most expensive product in the Fashion category: ID: {most_expensive_fashion['id']}, Name: {most_expensive_fashion['name']}, Price: {most_expensive_fashion['price']}")
        else:
            print("3. No products found in the Fashion category.")
        print(f"4. Average price of products in the 'Toys & Games' category: {average_price_toys_games:.2f} PLN")
        print(f"\nProducts have been saved to '{PRODUCTS_FILE}'")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()


"""Correct answer"""

# Analysis Results:
# 1. Total number of products: 204
# 2. Number of products in each category:
#    - Pet Supplies: 27
#    - Electronics: 19
#    - Toys & Games: 21
#    - Outdoor Equipment: 17
#    - Office Supplies: 15
#    - Sports Gear: 17
#    - Fashion: 20
#    - Automotive: 21
#    - Home Appliances: 22
#    - Health & Wellness: 25
# 3. Most expensive product in the Fashion category: ID: 187, Name: Pulsar Gadget, Price: 99603.91
# 4. Average price of products in the 'Toys & Games' category: 38463.45 PLN