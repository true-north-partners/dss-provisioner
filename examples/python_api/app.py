from __future__ import annotations

import argparse
from pathlib import Path

from dss_provisioner.config import apply, load, plan


def _progress(change: object, event: str) -> None:
    address = getattr(change, "address", "unknown")
    if event == "start":
        print(f"[apply:start] {address}")
    else:
        print(f"[apply:done]  {address}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Plan/apply dss-provisioner config via Python API")
    parser.add_argument("--config", default="dss-provisioner.yaml", help="Path to config file")
    parser.add_argument("--apply", action="store_true", help="Apply the generated plan")
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Skip refresh during plan",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load(config_path)

    plan_obj = plan(config, refresh=not args.no_refresh)
    print("Plan summary:", plan_obj.summary())
    for change in plan_obj.changes:
        print(f"- {change.action.value:6} {change.address}")

    if args.apply:
        result = apply(plan_obj, config, progress=_progress)
        print("Apply summary:", result.summary())


if __name__ == "__main__":
    main()
