"""Manual test script for ETL fetch functions."""

import asyncio
from backend.app.etl import fetch_items, fetch_logs


async def test_fetch():
    print("Testing fetch_items...")
    try:
        items = await fetch_items()
        print(f"✓ fetch_items: получено {len(items)} элементов")
        if items:
            print(f"  Пример: {items[0]}")
    except Exception as e:
        print(f"✗ fetch_items ошибка: {e}")

    print("\nTesting fetch_logs...")
    try:
        logs = await fetch_logs()
        print(f"✓ fetch_logs: получено {len(logs)} логов")
        if logs:
            print(f"  Пример: {logs[0]}")
    except Exception as e:
        print(f"✗ fetch_logs ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(test_fetch())
