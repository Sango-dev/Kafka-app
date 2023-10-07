from kafka import KafkaConsumer
import json
import time

def connect_to_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                'bitcoin_transactions',
                bootstrap_servers=['kafka:9093'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            return consumer
        except Exception as e:
            print("No brokers available...")
            time.sleep(20)

def consume_top_10_transactions():
    consumer = connect_to_kafka()
    
    top_10_transactions = []
    
    for message in consumer:
        transaction_data = message.value['data']
        price = float(transaction_data['price'])
        
        if not top_10_transactions or price >= top_10_transactions[-1]['price']:
            if len(top_10_transactions) == 10:
                top_10_transactions.pop()
            top_10_transactions.append({'price': price, 'data': transaction_data})
            top_10_transactions = sorted(top_10_transactions, key=lambda x: x['price'], reverse=True)
        
        print_top_10_transactions(top_10_transactions)

def print_top_10_transactions(top_10_transactions):
    print("\nTop 10 Transactions by Price:")
    for transaction in top_10_transactions:
        print(f"Price: {transaction['price']}, Data: {transaction['data']}")

if __name__ == "__main__":
    consume_top_10_transactions()