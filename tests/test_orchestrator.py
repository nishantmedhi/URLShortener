import asyncio
import tempfile
import unittest
from pathlib import Path

from orchestrator.engine import (
    Stage,
    WorkflowDefinition,
    WorkflowEngine,
    WorkflowError,
    WorkflowStore,
)
from orchestrator.example import SCENARIOS, build_workflow, handlers


def gate(context):
    return bool(context.get("request")), "request required"


def workflow(stages, exit_gate=lambda _: (True, "ok")):
    return WorkflowDefinition("test-flow", "1", tuple(stages), gate, exit_gate)


class WorkflowEngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = WorkflowStore(Path(self.temp.name) / "state.db")

    async def asyncTearDown(self):
        await self.store.close()
        self.temp.cleanup()

    async def approve_until_complete(self, engine, flow, run_id):
        decisions = []
        while True:
            state = await self.store.run(run_id)
            if state["status"] == "succeeded":
                return state, decisions
            pending = next(
                (name for name, node in state["nodes"].items() if node["status"] == "pending_approval"),
                None,
            )
            self.assertIsNotNone(pending)
            stage = flow.stage(pending)
            decisions.append(stage.approval_group)
            state = await engine.approve(
                flow,
                run_id,
                pending,
                actor=f"{stage.approval_group}-reviewer",
                approved=True,
                reason=f"{stage.approval_group} checks passed",
            )

    async def test_parallel_branches_wait_for_approval_and_record_lineage(self):
        flow = build_workflow()
        engine = WorkflowEngine(self.store, handlers())
        run_id = await engine.start(
            flow,
            {"request": "add feature", "change_control": "open", "scenario": "greenfield"},
        )
        state = await self.store.run(run_id)
        self.assertEqual(state["status"], "pending_approval")
        self.assertEqual(state["nodes"]["tests"]["status"], "succeeded")
        self.assertEqual(state["nodes"]["documentation"]["status"], "succeeded")
        self.assertEqual(state["nodes"]["security_approval"]["status"], "pending_approval")

        state, groups = await self.approve_until_complete(engine, flow, run_id)
        self.assertEqual(state["status"], "succeeded")
        self.assertEqual(groups, ["security", "compliance", "change_control", "release"])
        audit = await engine.audit(run_id)
        event_types = {event["event_type"] for event in audit}
        self.assertIn("approval.granted", event_types)
        self.assertIn("workflow.succeeded", event_types)
        approval_groups = {
            event["detail"]["approval_group"]
            for event in audit
            if event["event_type"] == "approval.granted"
        }
        self.assertEqual(approval_groups, set(groups))

    async def test_upstream_change_invalidates_outputs_and_requires_fresh_approval(self):
        flow = build_workflow()
        engine = WorkflowEngine(self.store, handlers())
        run_id = await engine.start(
            flow,
            {"request": "initial", "change_control": "open", "scenario": "greenfield"},
        )
        await self.approve_until_complete(engine, flow, run_id)
        state = await engine.replan(
            flow, run_id, {"request": "materially changed"}, actor="requester"
        )
        self.assertEqual(state["status"], "pending_approval")
        self.assertEqual(state["nodes"]["security_approval"]["status"], "pending_approval")
        self.assertEqual(state["nodes"]["release"]["status"], "pending")
        self.assertIsNone(state["nodes"]["release"]["approval"])
        events = [event["event_type"] for event in await engine.audit(run_id)]
        self.assertIn("stage.invalidated", events)
        self.assertIn("stage.replan_rolled_back", events)
        self.assertIn("workflow.replanned", events)
        metrics = await engine.metrics()
        self.assertEqual(metrics["rollback_count"], 4)
        self.assertEqual(metrics["rollback_actions_completed"], 4)

    async def test_retries_then_uses_fallback_and_reports_metrics(self):
        calls = 0

        async def flaky(_):
            nonlocal calls
            calls += 1
            raise RuntimeError("temporary")

        async def fallback(_):
            return {"degraded": True}

        flow = workflow(
            [Stage("work", "primary", fallback_handler="fallback")]
        )
        engine = WorkflowEngine(
            self.store,
            {"primary": flaky, "fallback": fallback},
            max_retries=1,
            retry_backoff_seconds=0,
        )
        run_id = await engine.start(flow, {"request": "bounded retry"})
        state = await self.store.run(run_id)
        self.assertEqual(calls, 2)
        self.assertEqual(state["status"], "succeeded")
        self.assertEqual(state["nodes"]["work"]["status"], "fallback_succeeded")
        metrics = await engine.metrics()
        self.assertEqual(metrics["retry_count"], 1)
        self.assertEqual(metrics["success_rate"], 1)

    async def test_failure_rolls_back_completed_stage(self):
        rolled_back = []

        async def run_stage(_):
            return {"changed": True}

        async def fail(_):
            raise RuntimeError("cannot continue")

        async def rollback(_):
            rolled_back.append(True)
            return {"restored": True}

        flow = workflow(
            [
                Stage("change", "change", rollback_handler="rollback"),
                Stage("verify", "verify", ("change",)),
            ]
        )
        engine = WorkflowEngine(
            self.store,
            {"change": run_stage, "verify": fail, "rollback": rollback},
            max_retries=0,
            retry_backoff_seconds=0,
        )
        run_id = await engine.start(flow, {"request": "failure"})
        state = await self.store.run(run_id)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["nodes"]["change"]["status"], "rolled_back")
        self.assertEqual(rolled_back, [True])
        self.assertEqual((await engine.metrics())["rollback_count"], 1)

    async def test_policy_blocks_frozen_change_without_running_handler(self):
        called = []

        async def handler(_):
            called.append(True)
            return {"ok": True}

        flow = workflow([Stage("change", "work")])
        engine = WorkflowEngine(self.store, {"work": handler})
        run_id = await engine.start(
            flow, {"request": "frozen", "change_control": "frozen"}
        )
        state = await self.store.run(run_id)
        self.assertEqual(state["status"], "failed")
        self.assertFalse(called)
        event_types = [event["event_type"] for event in await engine.audit(run_id)]
        self.assertIn("policy.blocked", event_types)

    async def test_replan_fails_closed_when_compensation_fails(self):
        flow = build_workflow()
        engine = WorkflowEngine(self.store, handlers())
        run_id = await engine.start(
            flow,
            {"request": "initial", "change_control": "open", "scenario": "greenfield"},
        )
        await self.approve_until_complete(engine, flow, run_id)

        async def broken_rollback(_):
            raise RuntimeError("rollback unavailable")

        engine.handlers["rollback"] = broken_rollback
        with self.assertRaisesRegex(WorkflowError, "compensation failed"):
            await engine.replan(
                flow, run_id, {"request": "changed"}, actor="requester"
            )
        state = await self.store.run(run_id)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["nodes"]["release"]["status"], "rollback_failed")

    async def test_scenario_entry_gates_cover_greenfield_brownfield_and_ambiguity(self):
        contexts = {
            "greenfield": {"request": "Create a short-link service"},
            "brownfield": {
                "request": "Add expiration handling",
                "affected_modules": ["app/main.py", "db/init.sql"],
            },
            "ambiguous": {
                "request": "Support custom URLs",
                "assumptions": ["Custom aliases are unique and case-sensitive"],
            },
        }
        self.assertEqual(set(contexts), set(SCENARIOS))
        for scenario, context in contexts.items():
            flow = build_workflow(scenario)
            engine = WorkflowEngine(self.store, handlers())
            run_id = await engine.start(
                flow,
                {
                    **context,
                    "scenario": scenario,
                    "change_control": "open",
                },
            )
            state = await self.store.run(run_id)
            self.assertEqual(state["status"], "pending_approval", scenario)
            self.assertEqual(state["nodes"]["requirements"]["status"], "succeeded")

        rejected_cases = [
            ("brownfield", {"request": "Change code"}),
            ("ambiguous", {"request": "Maybe add destinations"}),
        ]
        for scenario, context in rejected_cases:
            flow = build_workflow(scenario)
            run_id = await WorkflowEngine(self.store, handlers()).start(
                flow,
                {**context, "scenario": scenario, "change_control": "open"},
            )
            self.assertEqual((await self.store.run(run_id))["status"], "failed")

    async def test_approval_rejection_stops_downstream_release(self):
        flow = build_workflow()
        engine = WorkflowEngine(self.store, handlers())
        run_id = await engine.start(
            flow,
            {"request": "change", "scenario": "greenfield", "change_control": "open"},
        )
        state = await engine.approve(
            flow,
            run_id,
            "security_approval",
            actor="security-reviewer",
            approved=False,
            reason="unresolved critical finding",
        )
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["nodes"]["release"]["status"], "pending")
        events = [event["event_type"] for event in await engine.audit(run_id)]
        self.assertIn("approval.rejected", events)

    async def test_graph_rejects_unapproved_high_impact_and_cycles(self):
        with self.assertRaises(ValueError):
            workflow([Stage("release", "work", high_impact=True)])
        with self.assertRaises(ValueError):
            workflow(
                [
                    Stage("a", "a", ("b",)),
                    Stage("b", "b", ("a",)),
                ]
            )

    async def test_entry_gate_stops_before_any_execution(self):
        called = []

        async def handler(_):
            called.append(True)
            return {}

        flow = workflow([Stage("work", "work")])
        engine = WorkflowEngine(self.store, {"work": handler})
        run_id = await engine.start(flow, {"request": ""})
        state = await self.store.run(run_id)
        self.assertEqual(state["status"], "failed")
        self.assertFalse(called)

    async def test_independent_stages_run_in_parallel(self):
        both_started = asyncio.Event()
        count = 0

        async def parallel(_):
            nonlocal count
            count += 1
            if count == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=1)
            return {"ok": True}

        flow = workflow(
            [Stage("left", "parallel"), Stage("right", "parallel")]
        )
        engine = WorkflowEngine(self.store, {"parallel": parallel})
        run_id = await engine.start(flow, {"request": "parallel"})
        state = await self.store.run(run_id)
        self.assertEqual(state["status"], "succeeded")

    async def test_safe_stop_is_audited_and_prevents_approval_resume(self):
        flow = build_workflow()
        engine = WorkflowEngine(self.store, handlers())
        run_id = await engine.start(flow, {"request": "stop", "change_control": "open"})
        state = await engine.safe_stop(run_id, actor="operator", reason="change freeze")
        self.assertEqual(state["status"], "safe_stopped")
        with self.assertRaises(WorkflowError):
            await engine.approve(
                flow, run_id, "release", actor="manager", approved=True, reason="late"
            )


if __name__ == "__main__":
    unittest.main()
