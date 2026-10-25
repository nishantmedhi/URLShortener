import argparse
import asyncio
import json
import os
from typing import Any

from orchestrator.engine import WorkflowEngine, WorkflowError, WorkflowStore
from orchestrator.example import SCENARIOS, build_workflow, handlers


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Governed URL shortener SDLC workflow")
    root.add_argument("--db", default=os.getenv("WORKFLOW_DB_PATH", "data\\workflows.db"))
    root.add_argument("--scenario", choices=sorted(SCENARIOS), default="greenfield")
    commands = root.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start")
    start.add_argument("--request", required=True)
    start.add_argument("--change-control", choices=("open", "frozen"), default="open")
    start.add_argument("--affected-module", action="append", default=[])
    start.add_argument("--assumption", action="append", default=[])

    approve = commands.add_parser("approve")
    approve.add_argument("run_id")
    approve.add_argument("--stage", default="release")
    approve.add_argument("--actor", required=True)
    approve.add_argument("--reason", required=True)
    decision = approve.add_mutually_exclusive_group(required=True)
    decision.add_argument("--approve", action="store_true")
    decision.add_argument("--reject", action="store_true")

    status = commands.add_parser("status")
    status.add_argument("run_id")
    replan = commands.add_parser("replan")
    replan.add_argument("run_id")
    replan.add_argument("--request", required=True)
    replan.add_argument("--change-control", choices=("open", "frozen"))
    replan.add_argument("--affected-module", action="append")
    replan.add_argument("--assumption", action="append")
    replan.add_argument("--actor", required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("run_id")
    stop = commands.add_parser("stop")
    stop.add_argument("run_id")
    stop.add_argument("--actor", required=True)
    stop.add_argument("--reason", required=True)
    commands.add_parser("metrics")
    return root


async def execute(args: argparse.Namespace) -> Any:
    store = WorkflowStore(args.db)
    engine = WorkflowEngine(store, handlers())
    workflow = build_workflow(args.scenario)
    try:
        if args.command == "start":
            context = {
                "request": args.request,
                "change_control": args.change_control,
                "scenario": args.scenario,
            }
            if args.affected_module:
                context["affected_modules"] = args.affected_module
            if args.assumption:
                context["assumptions"] = args.assumption
            run_id = await engine.start(
                workflow,
                context,
            )
            return {"run_id": run_id, **await store.run(run_id)}
        if args.command == "approve":
            return await engine.approve(
                workflow,
                args.run_id,
                args.stage,
                actor=args.actor,
                approved=args.approve,
                reason=args.reason,
            )
        if args.command == "status":
            return await store.run(args.run_id)
        if args.command == "replan":
            patch = {"request": args.request}
            if args.change_control:
                patch["change_control"] = args.change_control
            if args.affected_module is not None:
                patch["affected_modules"] = args.affected_module
            if args.assumption is not None:
                patch["assumptions"] = args.assumption
            return await engine.replan(
                workflow, args.run_id, patch, actor=args.actor
            )
        if args.command == "audit":
            return await engine.audit(args.run_id)
        if args.command == "stop":
            return await engine.safe_stop(args.run_id, actor=args.actor, reason=args.reason)
        if args.command == "metrics":
            return await engine.metrics()
        raise WorkflowError(f"Unsupported command {args.command!r}")
    finally:
        await store.close()


def main() -> None:
    args = parser().parse_args()
    result = asyncio.run(execute(args))
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
