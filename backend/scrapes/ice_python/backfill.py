from __future__ import annotations

import importlib
from datetime import datetime, timedelta

API_SCRAPE_NAME = "ice_python_backfill"


def _load_module(module_name: str):
    return importlib.import_module(module_name)


def main(
    run_next_day_gas: bool = False,
    run_balmo: bool = False,
    run_future_contracts: bool = False,
    run_future_contracts_v2: bool = True,
) -> None:
    executed: list[str] = []
    try:
        print(f"\n=== {API_SCRAPE_NAME} ===\n")

        if run_next_day_gas:
            print("Backfilling next day gas...")
            next_day_gas_module = _load_module(
                "backend.scrapes.ice_python.next_day_gas.next_day_gas_v1_2025_dec_16"
            )
            next_day_gas_module.main(
                start_date=datetime(2013, 1, 1) - timedelta(days=1),
                end_date=datetime.now() + timedelta(days=1),
            )
            executed.append("next_day_gas_v1_2025_dec_16")

        if run_balmo:
            print("Backfilling balmo...")
            balmo_module = _load_module(
                "backend.scrapes.ice_python.balmo.balmo_v1_2025_dec_16"
            )
            balmo_module.main(
                start_date=datetime(2015, 1, 1) - timedelta(days=1),
                end_date=datetime.now() + timedelta(days=1),
            )
            executed.append("balmo_v1_2025_dec_16")

        if run_future_contracts:
            print("Backfilling future contracts (v1)...")
            future_contracts_module = _load_module(
                "backend.scrapes.ice_python.future_contracts.future_contracts_v1_2025_dec_16"
            )
            future_contracts_module.main(
                start_date=datetime(2019, 1, 1),
                end_date=datetime(2028, 12, 31),
                include_expired=True,
                contract_start_year=2020,
                contract_end_year=2028,
            )
            executed.append("future_contracts_v1_2025_dec_16")

        if run_future_contracts_v2:
            print("Backfilling future contracts (v2)...")
            future_contracts_v2_module = _load_module(
                "backend.scrapes.ice_python.future_contracts.future_contracts_v2_2026_mar_10"
            )
            future_contracts_v2_module.main(
                start_date=datetime(2019, 1, 1),
                end_date=datetime(2028, 12, 31),
                include_expired=True,
                contract_start_year=2020,
                contract_end_year=2028,
            )
            executed.append("future_contracts_v2_2026_mar_10")

        print(f"Completed pipelines: {', '.join(executed) if executed else 'none'}")

    except Exception as exc:
        print(f"Backfill failed after running: {', '.join(executed) if executed else 'none'}")
        raise


if __name__ == "__main__":
    main()
