
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from transaction_generator import generate_transaction


KAFKA_SERVER = "localhost:9092"
TOPIC = "transactions"
INTERVAL = 3


def create_producer():
    """Conecta o Producer ao servidor Kafka."""

    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda value: json.dumps(
                    value,
                    ensure_ascii=False
                ).encode("utf-8"),
            )

            print("Conectado ao Kafka com sucesso!")
            return producer

        except KafkaError as error:
            print(f"Kafka indisponível: {error}")
            print("Tentando novamente em 5 segundos...")
            time.sleep(5)


def main():
    producer = create_producer()

    print(f"Enviando transações para o tópico '{TOPIC}'...")
    print("Pressione CTRL+C para encerrar.\n")

    try:
        while True:
            transaction = generate_transaction()

            try:
                future = producer.send(TOPIC, value=transaction)
                metadata = future.get(timeout=10)

                print(
                    f"Transação enviada | "
                    f"Cliente: {transaction['customer_id']} | "
                    f"Valor: R$ {transaction['amount']:.2f} | "
                    f"Cidade: {transaction['city']} | "
                    f"Tópico: {metadata.topic}"
                )

            except KafkaError as error:
                print(f"Erro ao enviar transação: {error}")

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nEncerrando o Producer...")

    finally:
        producer.close()
        print("Producer encerrado.")


if __name__ == "__main__":
    main()
