from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd

from fetchgraph.relational_schema import build_sql_provider_from_schema

from .schema import RETAIL_SCHEMA


def _load_csv_into_sql(conn: sqlite3.Connection, data_dir: Path) -> None:
    for ent in RETAIL_SCHEMA.entities:
        if not ent.source:
            continue
        csv_path = data_dir / ent.source
        df = pd.read_csv(csv_path)
        table_name = Path(ent.source).stem
        df.to_sql(table_name, conn, if_exists="replace", index=False)


def main() -> None:
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"

    # 1) Создаём SQLite-подключение и загружаем туда те же CSV, что использует Pandas-демо
    with sqlite3.connect(":memory:") as conn:
        _load_csv_into_sql(conn, data_dir)

        # 2) Собираем провайдер SQL на основе той же SchemaConfig
        retail_provider = build_sql_provider_from_schema(conn, RETAIL_SCHEMA)

        # 3) Показываем описание провайдера (идентично Pandas-варианту)
        info = retail_provider.describe()
        print("=== ProviderInfo.name ===")
        print(info.name)
        print("\n=== ProviderInfo.description ===")
        print(info.description)
        print("\n=== ProviderInfo.capabilities ===")
        print(info.capabilities)
        print("\n=== ProviderInfo.examples ===")
        for ex in info.examples or []:
            print("-", ex)
        # 4) В реальном приложении можно собрать BaseGraphAgent (см. README)
        #    и использовать этот же провайдер для SQL-выполнения.


if __name__ == "__main__":
    main()
