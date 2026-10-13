
import json
import time
from datetime import datetime

import psycopg
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from fraud_detector import FraudDetector


KAFKA_SERVER = "localhost:9092"
TOPIC = "transactions"

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "fraudes_db",
    "user": "fraude_user",
    "password": "dev_fraude_password",
}

console = Console()


def connect_database():
    """Conecta ao PostgreSQL, tentando novamente em caso de falha."""
    while True:
        try:
            connection = psycopg.connect(**DB_CONFIG)
            console.print("[green]✓ PostgreSQL conectado![/green]")
            return connection
        except psycopg.OperationalError as error:
            console.print(f"[red]PostgreSQL indisponível: {error}[/red]")
            console.print("[yellow]Nova tentativa em 5 segundos...[/yellow]")
            time.sleep(5)


def create_consumer():
    """Conecta ao Kafka e assina o tópico de transações."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda message: json.loads(
                    message.decode("utf-8")
                ),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id="fraud-detection-group",
            )
            console.print("[green]✓ Kafka conectado![/green]")
            console.print(f"[cyan]Tópico: {TOPIC}[/cyan]")
            return consumer
        except KafkaError as error:
            console.print(f"[red]Kafka indisponível: {error}[/red]")
            console.print("[yellow]Nova tentativa em 5 segundos...[/yellow]")
            time.sleep(5)


def save_transaction(connection, transaction, alerts):
    """Salva a transação e seus alertas no banco."""
    query = """
        INSERT INTO transactions (
            transaction_id,
            customer_id,
            amount,
            city,
            transaction_timestamp,
            is_suspicious,
            fraud_types
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING
    """

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                transaction["transaction_id"],
                transaction["customer_id"],
                transaction["amount"],
                transaction["city"],
                transaction["timestamp"],
                bool(alerts),
                json.dumps(alerts),
            ),
        )

    connection.commit()


def format_currency(amount):
    """Formata valores no padrão brasileiro."""
    formatted = f"{float(amount):,.2f}"
    formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


def display_header():
    """Exibe o cabeçalho do monitor."""
    header = Text()
    header.append("MONITOR DE FRAUDES\n", style="bold cyan")
    header.append("Kafka  |  PostgreSQL  |  Tempo real", style="dim")

    console.print(
        Panel(
            header,
            border_style="cyan",
            box=box.DOUBLE,
            padding=(1, 2),
        )
    )


def display_transaction(transaction, alerts, totals):
    """Exibe uma transação em uma tabela organizada."""
    suspicious = bool(alerts)

    table = Table(
        title="TRANSAÇÃO PROCESSADA",
        box=box.ROUNDED,
        border_style="red" if suspicious else "green",
        show_lines=True,
    )

    table.add_column("Campo", style="cyan", no_wrap=True)
    table.add_column("Informação", overflow="fold")

    table.add_row("Cliente", transaction["customer_id"])
    table.add_row("Valor", format_currency(transaction["amount"]))
    table.add_row("Cidade", transaction["city"])
    table.add_row(
        "ID",
        Text(transaction["transaction_id"], style="dim"),
    )

    if suspicious:
        table.add_row(
            "Status",
            Text("SUSPEITA DETECTADA", style="bold red"),
        )
        table.add_row(
            "Alertas",
            Text(" | ".join(alerts), style="bold yellow"),
        )
        totals["suspicious"] += 1
    else:
        table.add_row(
            "Status",
            Text("SEM ALERTAS", style="bold green"),
        )

    totals["processed"] += 1

    console.print()
    console.print(table)

    summary = (
        f"[bold]Processadas:[/bold] {totals['processed']}    "
        f"[bold red]Suspeitas:[/bold red] {totals['suspicious']}    "
        f"[bold green]Sem alertas:[/bold green] "
        f"{totals['processed'] - totals['suspicious']}"
    )
    console.print(Panel(summary, border_style="blue", padding=(0, 1)))


def main():
    display_header()

    connection = connect_database()
    consumer = create_consumer()
    detector = FraudDetector()

    totals = {"processed": 0, "suspicious": 0}

    console.print()
    console.print(
        Panel(
            "[bold green]Monitoramento iniciado[/bold green]\n"
            "Pressione [bold]CTRL+C[/bold] para encerrar.",
            border_style="green",
        )
    )

    try:
        for message in consumer:
            transaction = message.value

            try:
                alerts = detector.detect(transaction)
                save_transaction(connection, transaction, alerts)

                display_transaction(transaction, alerts, totals)

                # Confirma a mensagem somente depois de salvar no banco.
                consumer.commit()

            except (
                KeyError,
                ValueError,
                TypeError,
                psycopg.Error,
            ) as error:
                connection.rollback()
                console.print(
                    f"\n[bold red]Erro ao processar transação:[/bold red] "
                    f"{error}"
                )

    except KeyboardInterrupt:
        console.print("\n[yellow]Encerrando o monitoramento...[/yellow]")

    finally:
        consumer.close()
        connection.close()

        console.print(
            Panel(
                f"[bold]Total processado:[/bold] {totals['processed']}\n"
                f"[bold red]Transações suspeitas:[/bold red] "
                f"{totals['suspicious']}\n"
                f"[bold green]Transações sem alertas:[/bold green] "
                f"{totals['processed'] - totals['suspicious']}",
                title="RESUMO FINAL",
                border_style="cyan",
            )
        )
        console.print("[dim]Consumer encerrado.[/dim]")


if __name__ == "__main__":
    main()
