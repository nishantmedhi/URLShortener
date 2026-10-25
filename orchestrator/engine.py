from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

logger = logging.getLogger(__name__)
Handler = Callable[[Mapping[str, Any]], Awaitable[Mapping[str, Any]]]
Gate = Callable[[Mapping[str, Any]], tuple[bool, str]]
REQUIRED_POLICIES = frozenset({"security", "compliance", "change_control"})
SUCCESS_STATES = {"succeeded", "fallback_succeeded"}
TERMINAL_STATES = {"succeeded", "failed", "safe_stopped"}


class WorkflowError(Exception):
    pass


class PolicyViolation(WorkflowError):
    pass


@dataclass(frozen=True)
class Stage:
    name: str
    handler: str
    depends_on: tuple[str, ...] = ()
    high_impact: bool = False
    approval_required: bool = False
    policy_tags: frozenset[str] = REQUIRED_POLICIES
    fallback_handler: str | None = None
    rollback_handler: str | None = None
    approval_group: str | None = None


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    version: str
    stages: tuple[Stage, ...]
    entry_gate: Gate
    exit_gate: Gate
    required_policy_tags: frozenset[str] = REQUIRED_POLICIES
    _stage_by_name: dict[str, Stage] = field(init=False, repr=False, compare=False)
    _topological_order: tuple[str, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        by_name = {stage.name: stage for stage in self.stages}
        if not self.name or not self.version or len(by_name) != len(self.stages):
            raise ValueError("Workflow name/version must be set and stage names must be unique")
        if not self.stages:
            raise ValueError("A workflow must include at least one stage")
        for stage in self.stages:
            missing = set(stage.depends_on) - by_name.keys()
            if missing:
                raise ValueError(f"Stage {stage.name!r} has unknown dependencies: {sorted(missing)}")
            if stage.name in stage.depends_on:
                raise ValueError(f"Stage {stage.name!r} cannot depend on itself")
            missing_policies = self.required_policy_tags - stage.policy_tags
            if missing_policies:
                raise ValueError(
                    f"Stage {stage.name!r} lacks required policy tags: {sorted(missing_policies)}"
                )
            if stage.high_impact and not stage.approval_required:
                raise ValueError(f"High-impact stage {stage.name!r} requires human approval")
            if stage.high_impact and not stage.rollback_handler:
                raise ValueError(f"High-impact stage {stage.name!r} requires a rollback handler")
            if stage.approval_required and not stage.approval_group:
                raise ValueError(f"Approval stage {stage.name!r} must declare an approval group")
        if not REQUIRED_POLICIES.issubset(self.required_policy_tags):
            raise ValueError(
                f"Workflow policy requirements cannot omit {sorted(REQUIRED_POLICIES)}"
            )

        ordered: list[str] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visiting:
                raise ValueError(f"Workflow contains a dependency cycle at {name!r}")
            if name in visited:
                return
            visiting.add(name)
            for dependency in by_name[name].depends_on:
                visit(dependency)
            visiting.remove(name)
            visited.add(name)
            ordered.append(name)

        for stage in self.stages:
            visit(stage.name)
        object.__setattr__(self, "_stage_by_name", by_name)
        object.__setattr__(self, "_topological_order", tuple(ordered))

    @property
    def topological_order(self) -> tuple[str, ...]:
        return self._topological_order

    def stage(self, name: str) -> Stage:
        return self._stage_by_name[name]


class WorkflowStore:
    """SQLite event/state store; WAL enables concurrent readers and durable local runs."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS workflow_runs (
                run_id TEXT PRIMARY KEY,
                workflow_name TEXT NOT NULL,
                workflow_version TEXT NOT NULL,
                status TEXT NOT NULL,
                context_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                finished_at REAL
            );
            CREATE TABLE IF NOT EXISTS workflow_nodes (
                run_id TEXT NOT NULL REFERENCES workflow_runs(run_id),
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                input_fingerprint TEXT,
                output_json TEXT,
                error TEXT,
                approval_json TEXT,
                updated_at REAL NOT NULL,
                recovery_seconds REAL,
                PRIMARY KEY (run_id, name)
            );
            CREATE TABLE IF NOT EXISTS workflow_audit (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES workflow_runs(run_id),
                occurred_at REAL NOT NULL,
                event_type TEXT NOT NULL,
                stage TEXT,
                actor TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS workflow_audit_run_idx
                ON workflow_audit(run_id, sequence);
            """
        )
        self.connection.commit()
        self.lock = asyncio.Lock()

    async def close(self) -> None:
        async with self.lock:
            self.connection.close()

    async def create_run(
        self, run_id: str, workflow: WorkflowDefinition, context: Mapping[str, Any], actor: str
    ) -> None:
        async with self.lock:
            now = time.time()
            with self.connection:
                self.connection.execute(
                    """INSERT INTO workflow_runs
                       (run_id, workflow_name, workflow_version, status, context_json, created_at, updated_at)
                       VALUES (?, ?, ?, 'pending', ?, ?, ?)""",
                    (run_id, workflow.name, workflow.version, _json(context), now, now),
                )
                self.connection.executemany(
                    """INSERT INTO workflow_nodes(run_id, name, status, updated_at)
                       VALUES (?, ?, 'pending', ?)""",
                    [(run_id, stage.name, now) for stage in workflow.stages],
                )
                self._audit(run_id, "workflow.created", None, actor, {
                    "workflow": workflow.name,
                    "version": workflow.version,
                    "stage_order": workflow.topological_order,
                })

    async def run(self, run_id: str) -> dict[str, Any]:
        async with self.lock:
            run = self.connection.execute(
                "SELECT * FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise WorkflowError(f"Unknown workflow run: {run_id}")
            nodes = self.connection.execute(
                "SELECT * FROM workflow_nodes WHERE run_id = ?", (run_id,)
            ).fetchall()
            return {
                **dict(run),
                "context": json.loads(run["context_json"]),
                "nodes": {node["name"]: _node_dict(node) for node in nodes},
            }

    async def set_run(
        self,
        run_id: str,
        status: str,
        actor: str,
        event_type: str,
        detail: Mapping[str, Any] | None = None,
        *,
        finished: bool = False,
    ) -> None:
        async with self.lock:
            now = time.time()
            with self.connection:
                self.connection.execute(
                    """UPDATE workflow_runs SET status=?, updated_at=?, finished_at=?
                       WHERE run_id=?""",
                    (status, now, now if finished else None, run_id),
                )
                self._audit(run_id, event_type, None, actor, detail or {})

    async def update_context(self, run_id: str, context: Mapping[str, Any], actor: str) -> None:
        async with self.lock:
            now = time.time()
            with self.connection:
                self.connection.execute(
                    "UPDATE workflow_runs SET context_json=?, updated_at=? WHERE run_id=?",
                    (_json(context), now, run_id),
                )
                self._audit(
                    run_id,
                    "workflow.replanned",
                    None,
                    actor,
                    {
                        "context_hash": _fingerprint(context),
                        "context_keys": sorted(context),
                    },
                )

    async def set_node(
        self,
        run_id: str,
        name: str,
        status: str,
        actor: str,
        event_type: str,
        detail: Mapping[str, Any] | None = None,
        *,
        attempts: int | None = None,
        fingerprint: str | None = None,
        output: Mapping[str, Any] | None = None,
        error: str | None = None,
        approval: Mapping[str, Any] | None = None,
        clear_approval: bool = False,
        recovery_seconds: float | None = None,
    ) -> None:
        async with self.lock:
            now = time.time()
            with self.connection:
                self.connection.execute(
                    """UPDATE workflow_nodes
                       SET status=?, attempts=COALESCE(?, attempts),
                           input_fingerprint=COALESCE(?, input_fingerprint),
                           output_json=?, error=?,
                           approval_json=CASE WHEN ? THEN NULL ELSE COALESCE(?, approval_json) END,
                           updated_at=?, recovery_seconds=COALESCE(?, recovery_seconds)
                       WHERE run_id=? AND name=?""",
                    (
                        status,
                        attempts,
                        fingerprint,
                        _json(output) if output is not None else None,
                        error,
                        clear_approval,
                        _json(approval) if approval is not None else None,
                        now,
                        recovery_seconds,
                        run_id,
                        name,
                    ),
                )
                self.connection.execute(
                    "UPDATE workflow_runs SET updated_at=? WHERE run_id=?", (now, run_id)
                )
                self._audit(run_id, event_type, name, actor, detail or {})

    async def audit(self, run_id: str) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.connection.execute(
                """SELECT sequence, occurred_at, event_type, stage, actor, detail_json
                   FROM workflow_audit WHERE run_id=? ORDER BY sequence""",
                (run_id,),
            ).fetchall()
            return [
                {**dict(row), "detail": json.loads(row["detail_json"])}
                for row in rows
            ]

    async def metrics(self) -> dict[str, float | int]:
        async with self.lock:
            runs = self.connection.execute(
                """SELECT status, created_at, finished_at FROM workflow_runs
                   WHERE status IN ('succeeded', 'failed', 'safe_stopped')"""
            ).fetchall()
            retries = self.connection.execute(
                "SELECT count(*) FROM workflow_audit WHERE event_type='stage.retry'"
            ).fetchone()[0]
            rollback_count = self.connection.execute(
                """SELECT count(*) FROM workflow_audit
                   WHERE event_type IN ('workflow.rollback_started', 'workflow.replan_rollback_started')"""
            ).fetchone()[0]
            rollback_actions = self.connection.execute(
                """SELECT count(*) FROM workflow_audit
                   WHERE event_type IN ('stage.rolled_back', 'stage.replan_rolled_back')"""
            ).fetchone()[0]
            recoveries = self.connection.execute(
                "SELECT recovery_seconds FROM workflow_nodes WHERE recovery_seconds IS NOT NULL"
            ).fetchall()
            durations = [
                max(0.0, run["finished_at"] - run["created_at"])
                for run in runs
                if run["finished_at"] is not None
            ]
            sorted_durations = sorted(durations)
            p95_index = max(0, int(len(sorted_durations) * 0.95 + 0.999999) - 1)
            return {
                "completed_runs": len(runs),
                "success_rate": (
                    sum(run["status"] == "succeeded" for run in runs) / len(runs)
                    if runs
                    else 0.0
                ),
                "retry_count": retries,
                "rollback_count": rollback_count,
                "rollback_actions_completed": rollback_actions,
                "retry_frequency_per_run": retries / len(runs) if runs else 0.0,
                "rollback_frequency_per_run": rollback_count / len(runs) if runs else 0.0,
                "mttr_seconds": (
                    sum(row["recovery_seconds"] for row in recoveries) / len(recoveries)
                    if recoveries
                    else 0.0
                ),
                "end_to_end_latency_seconds_avg": (
                    sum(durations) / len(durations) if durations else 0.0
                ),
                "end_to_end_latency_seconds_p95": (
                    sorted_durations[p95_index] if sorted_durations else 0.0
                ),
            }

    def _audit(
        self, run_id: str, event_type: str, stage: str | None, actor: str, detail: Mapping[str, Any]
    ) -> None:
        self.connection.execute(
            """INSERT INTO workflow_audit(run_id, occurred_at, event_type, stage, actor, detail_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, time.time(), event_type, stage, actor, _json(detail)),
        )


class WorkflowEngine:
    def __init__(
        self,
        store: WorkflowStore,
        handlers: Mapping[str, Handler],
        *,
        max_retries: int = 2,
        action_timeout_seconds: float = 30.0,
        retry_backoff_seconds: float = 0.1,
    ):
        if max_retries < 0 or action_timeout_seconds <= 0 or retry_backoff_seconds < 0:
            raise ValueError("Retry count and timeout/backoff settings must be non-negative")
        self.store = store
        self.handlers = dict(handlers)
        self.max_retries = max_retries
        self.action_timeout_seconds = action_timeout_seconds
        self.retry_backoff_seconds = retry_backoff_seconds
        self._stop_events: dict[str, asyncio.Event] = {}
        self._run_locks: dict[str, asyncio.Lock] = {}

    async def start(
        self, workflow: WorkflowDefinition, context: Mapping[str, Any], actor: str = "system"
    ) -> str:
        self._validate_handlers(workflow)
        passed, reason = workflow.entry_gate(context)
        run_id = str(uuid.uuid4())
        await self.store.create_run(run_id, workflow, context, actor)
        if not passed:
            await self.store.set_run(
                run_id, "failed", actor, "workflow.entry_gate_rejected",
                {"reason": reason}, finished=True,
            )
            return run_id
        await self.store.set_run(run_id, "running", actor, "workflow.entry_gate_passed")
        await self.run(workflow, run_id, actor=actor)
        return run_id

    async def run(
        self, workflow: WorkflowDefinition, run_id: str, actor: str = "system"
    ) -> dict[str, Any]:
        self._validate_handlers(workflow)
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            return await self._run_locked(workflow, run_id, actor)

    async def approve(
        self,
        workflow: WorkflowDefinition,
        run_id: str,
        stage_name: str,
        *,
        actor: str,
        approved: bool,
        reason: str,
    ) -> dict[str, Any]:
        self._validate_handlers(workflow)
        stage = workflow.stage(stage_name)
        if not stage.approval_required:
            raise WorkflowError(f"Stage {stage_name!r} does not require approval")
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            state = await self.store.run(run_id)
            self._assert_run_definition(workflow, state)
            if state["status"] != "pending_approval":
                raise WorkflowError(f"Workflow run {run_id} is not accepting approvals")
            node = state["nodes"][stage_name]
            if node["status"] != "pending_approval":
                raise WorkflowError(f"Stage {stage_name!r} is not awaiting approval")
            decision = {
                "approved": approved,
                "actor": actor,
                "reason": reason,
                "decided_at": datetime.now(UTC).isoformat(),
                "approval_group": stage.approval_group,
            }
            if not actor.strip() or not reason.strip():
                raise WorkflowError("Approval decisions require a named actor and a non-empty reason")
            await self.store.set_node(
                run_id,
                stage_name,
                "pending" if approved else "rejected",
                actor,
                "approval.granted" if approved else "approval.rejected",
                decision,
                approval=decision,
            )
            if not approved:
                await self._rollback(workflow, run_id, actor, f"Approval rejected: {reason}")
                return await self.store.run(run_id)
            await self.store.set_run(run_id, "running", actor, "workflow.approval_resumed", decision)
            return await self._run_locked(workflow, run_id, actor)

    async def replan(
        self,
        workflow: WorkflowDefinition,
        run_id: str,
        context_patch: Mapping[str, Any],
        *,
        actor: str,
    ) -> dict[str, Any]:
        self._validate_handlers(workflow)
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            state = await self.store.run(run_id)
            self._assert_run_definition(workflow, state)
            if state["status"] in {"failed", "safe_stopped"}:
                raise WorkflowError("A failed or safely stopped workflow cannot be replanned")
            context = {**state["context"], **context_patch}
            if not await self._prepare_replan(workflow, run_id, state, context, actor):
                raise WorkflowError("Re-plan compensation failed; workflow stopped in a failed state")
            await self.store.update_context(run_id, context, actor)
            await self.store.set_run(
                run_id,
                "running",
                actor,
                "workflow.replan_resumed",
                {"previous_status": state["status"]},
            )
            return await self._run_locked(workflow, run_id, actor)

    async def safe_stop(self, run_id: str, *, actor: str, reason: str) -> dict[str, Any]:
        state = await self.store.run(run_id)
        if state["status"] in TERMINAL_STATES:
            raise WorkflowError(f"Workflow run {run_id} is already terminal")
        self._stop_events.setdefault(run_id, asyncio.Event()).set()
        await self.store.set_run(
            run_id, "safe_stopped", actor, "workflow.safe_stopped",
            {"reason": reason}, finished=True,
        )
        return await self.store.run(run_id)

    async def audit(self, run_id: str) -> list[dict[str, Any]]:
        return await self.store.audit(run_id)

    async def metrics(self) -> dict[str, float | int]:
        return await self.store.metrics()

    async def _run_locked(
        self, workflow: WorkflowDefinition, run_id: str, actor: str
    ) -> dict[str, Any]:
        state = await self.store.run(run_id)
        self._assert_run_definition(workflow, state)
        if state["status"] in TERMINAL_STATES or state["status"] == "pending_approval":
            return state
        stop_event = self._stop_events.setdefault(run_id, asyncio.Event())
        while not stop_event.is_set():
            state = await self.store.run(run_id)
            if state["status"] == "safe_stopped":
                return state
            await self._invalidate_stale_nodes(workflow, run_id, state, actor)
            state = await self.store.run(run_id)
            if any(node["status"] == "pending_approval" for node in state["nodes"].values()):
                await self.store.set_run(run_id, "pending_approval", actor, "workflow.awaiting_approval")
                return await self.store.run(run_id)

            ready = [
                workflow.stage(name)
                for name in workflow.topological_order
                if state["nodes"][name]["status"] == "pending"
                and all(state["nodes"][dep]["status"] in SUCCESS_STATES for dep in workflow.stage(name).depends_on)
            ]
            if not ready:
                if all(state["nodes"][name]["status"] in SUCCESS_STATES for name in workflow.topological_order):
                    outputs = {name: state["nodes"][name]["output"] for name in workflow.topological_order}
                    context = {**state["context"], "outputs": outputs}
                    passed, reason = workflow.exit_gate(context)
                    if not passed:
                        await self._rollback(workflow, run_id, actor, f"Exit gate rejected: {reason}")
                    else:
                        await self.store.set_run(
                            run_id, "succeeded", actor, "workflow.succeeded",
                            {"exit_gate": reason}, finished=True,
                        )
                    return await self.store.run(run_id)
                await self.store.set_run(
                    run_id, "failed", actor, "workflow.no_runnable_stages",
                    {"node_states": {key: value["status"] for key, value in state["nodes"].items()}},
                    finished=True,
                )
                await self._rollback(workflow, run_id, actor, "No runnable stages remain")
                return await self.store.run(run_id)

            for stage in ready:
                if stage.approval_required:
                    if state["nodes"][stage.name]["approval"] is None:
                        await self.store.set_node(
                            run_id,
                            stage.name,
                            "pending_approval",
                            actor,
                            "approval.requested",
                            {
                                "high_impact": stage.high_impact,
                                "approval_group": stage.approval_group,
                                "policy_tags": sorted(stage.policy_tags),
                                "dependency_output_hashes": {
                                    dep: _fingerprint(state["nodes"][dep]["output"])
                                    for dep in stage.depends_on
                                },
                            },
                            fingerprint=_fingerprint(
                                {
                                    "context": state["context"],
                                    "dependencies": {
                                        dep: state["nodes"][dep]["output"] for dep in stage.depends_on
                                    },
                                }
                            ),
                        )
                        await self.store.set_run(
                            run_id, "pending_approval", actor, "workflow.awaiting_approval",
                            {"stage": stage.name},
                        )
                        return await self.store.run(run_id)
                    if not state["nodes"][stage.name]["approval"]["approved"]:
                        continue

            batch = [stage for stage in ready if not stage.approval_required or state["nodes"][stage.name]["approval"]]
            if not batch:
                continue
            results = await asyncio.gather(
                *(self._execute_stage(workflow, stage, run_id, state, actor) for stage in batch),
                return_exceptions=True,
            )
            if stop_event.is_set():
                await self.store.set_run(
                    run_id, "safe_stopped", actor, "workflow.safe_stopped",
                    {"reason": "Safe-stop requested during stage execution"}, finished=True,
                )
                return await self.store.run(run_id)
            failures = [result for result in results if isinstance(result, BaseException)]
            if failures:
                await self._rollback(workflow, run_id, actor, str(failures[0]))
                return await self.store.run(run_id)

        await self.store.set_run(
            run_id, "safe_stopped", actor, "workflow.safe_stopped",
            {"reason": "Safe-stop requested"}, finished=True,
        )
        return await self.store.run(run_id)

    async def _execute_stage(
        self,
        workflow: WorkflowDefinition,
        stage: Stage,
        run_id: str,
        state: Mapping[str, Any],
        actor: str,
    ) -> None:
        try:
            self._enforce_policy(stage, state["context"])
        except PolicyViolation as exc:
            await self.store.set_node(
                run_id,
                stage.name,
                "failed",
                actor,
                "policy.blocked",
                {"reason": str(exc)},
                error=str(exc),
            )
            raise
        dependency_outputs = {
            name: state["nodes"][name]["output"] for name in stage.depends_on
        }
        input_data = {"context": state["context"], "dependencies": dependency_outputs}
        fingerprint = _fingerprint(input_data)
        started = time.monotonic()
        previous_attempts = state["nodes"][stage.name]["attempts"]
        attempts = 0
        first_error: Exception | None = None
        for attempts in range(1, self.max_retries + 2):
            await self.store.set_node(
                run_id,
                stage.name,
                "running",
                actor,
                "stage.started" if attempts == 1 else "stage.retry",
                {"attempt": attempts, "max_attempts": self.max_retries + 1},
                attempts=previous_attempts + attempts,
                fingerprint=fingerprint,
                error=None,
            )
            try:
                output = await asyncio.wait_for(
                    self.handlers[stage.handler](input_data),
                    timeout=self.action_timeout_seconds,
                )
                if not isinstance(output, Mapping):
                    raise WorkflowError(f"Handler {stage.handler!r} must return a mapping")
                await self.store.set_node(
                    run_id,
                    stage.name,
                    "succeeded",
                    actor,
                    "stage.succeeded",
                    {"attempt": attempts, "output_hash": _fingerprint(output)},
                    attempts=previous_attempts + attempts,
                    fingerprint=fingerprint,
                    output=output,
                    recovery_seconds=time.monotonic() - started if attempts > 1 else None,
                )
                return
            except PolicyViolation as exc:
                first_error = exc
                break
            except Exception as exc:
                first_error = exc
                if attempts <= self.max_retries:
                    delay = min(self.retry_backoff_seconds * (2 ** (attempts - 1)), 5.0)
                    if delay:
                        await asyncio.sleep(delay)

        error_message = f"{type(first_error).__name__}: {first_error}"
        if stage.fallback_handler:
            try:
                fallback_output = await asyncio.wait_for(
                    self.handlers[stage.fallback_handler](input_data),
                    timeout=self.action_timeout_seconds,
                )
                if not isinstance(fallback_output, Mapping):
                    raise WorkflowError(f"Fallback {stage.fallback_handler!r} must return a mapping")
                await self.store.set_node(
                    run_id,
                    stage.name,
                    "fallback_succeeded",
                    actor,
                    "stage.fallback_succeeded",
                    {"error": error_message, "fallback": stage.fallback_handler},
                    attempts=previous_attempts + attempts,
                    fingerprint=fingerprint,
                    output=fallback_output,
                    error=error_message,
                    recovery_seconds=time.monotonic() - started,
                )
                return
            except Exception as exc:
                error_message += f"; fallback failed: {type(exc).__name__}: {exc}"
        await self.store.set_node(
            run_id,
            stage.name,
            "failed",
            actor,
            "stage.failed",
            {"attempts": attempts, "error": error_message},
            attempts=previous_attempts + attempts,
            fingerprint=fingerprint,
            error=error_message,
        )
        raise WorkflowError(f"Stage {stage.name!r} failed: {error_message}")

    async def _invalidate_stale_nodes(
        self,
        workflow: WorkflowDefinition,
        run_id: str,
        state: Mapping[str, Any],
        actor: str,
    ) -> None:
        for name in workflow.topological_order:
            stage = workflow.stage(name)
            node = state["nodes"][name]
            if node["status"] not in SUCCESS_STATES:
                continue
            if not all(state["nodes"][dep]["status"] in SUCCESS_STATES for dep in stage.depends_on):
                continue
            expected = _fingerprint(
                {
                    "context": state["context"],
                    "dependencies": {dep: state["nodes"][dep]["output"] for dep in stage.depends_on},
                }
            )
            if node["input_fingerprint"] != expected:
                await self.store.set_node(
                    run_id,
                    name,
                    "pending",
                    actor,
                    "stage.invalidated",
                    {
                        "previous_fingerprint": node["input_fingerprint"],
                        "new_fingerprint": expected,
                        "reason": "upstream output or workflow context changed",
                    },
                    fingerprint=expected,
                    output={},
                    error=None,
                    clear_approval=True,
                )

    async def _prepare_replan(
        self,
        workflow: WorkflowDefinition,
        run_id: str,
        state: Mapping[str, Any],
        new_context: Mapping[str, Any],
        actor: str,
    ) -> bool:
        affected = []
        for name in workflow.topological_order:
            stage = workflow.stage(name)
            node = state["nodes"][name]
            if node["status"] not in SUCCESS_STATES | {"pending_approval"}:
                continue
            fingerprint = _fingerprint(
                {
                    "context": new_context,
                    "dependencies": {dep: state["nodes"][dep]["output"] for dep in stage.depends_on},
                }
            )
            if node["input_fingerprint"] != fingerprint:
                affected.append((stage, node, fingerprint))

        for stage, node, fingerprint in reversed(affected):
            if node["status"] in SUCCESS_STATES and stage.rollback_handler:
                await self.store.set_run(
                    run_id,
                    "running",
                    actor,
                    "workflow.replan_rollback_started",
                    {"stage": stage.name, "handler": stage.rollback_handler},
                )
                try:
                    await asyncio.wait_for(
                        self.handlers[stage.rollback_handler](
                            {
                                "context": state["context"],
                                "output": node["output"],
                                "cause": "upstream context changed; compensating before re-plan",
                            }
                        ),
                        timeout=self.action_timeout_seconds,
                    )
                except Exception as exc:
                    message = f"{type(exc).__name__}: {exc}"
                    logger.exception(
                        "Re-plan compensation failed run_id=%s stage=%s",
                        run_id,
                        stage.name,
                    )
                    await self.store.set_node(
                        run_id,
                        stage.name,
                        "rollback_failed",
                        actor,
                        "stage.rollback_failed",
                        {"error": message, "reason": "re-plan compensation"},
                        error=message,
                    )
                    await self.store.set_run(
                        run_id,
                        "failed",
                        actor,
                        "workflow.replan_rollback_failed",
                        {"stage": stage.name, "error": message},
                        finished=True,
                    )
                    return False
                event_type = "stage.replan_rolled_back"
            else:
                event_type = "stage.invalidated"
            await self.store.set_node(
                run_id,
                stage.name,
                "pending",
                actor,
                event_type,
                {
                    "previous_fingerprint": node["input_fingerprint"],
                    "new_fingerprint": fingerprint,
                    "reason": "upstream output or workflow context changed",
                    "rollback_handler": stage.rollback_handler
                    if node["status"] in SUCCESS_STATES
                    else None,
                },
                fingerprint=fingerprint,
                output={},
                error=None,
                clear_approval=True,
            )
        return True

    async def _rollback(
        self, workflow: WorkflowDefinition, run_id: str, actor: str, cause: str
    ) -> None:
        await self.store.set_run(
            run_id, "rolling_back", actor, "workflow.rollback_started", {"cause": cause}
        )
        state = await self.store.run(run_id)
        rollback_failed = False
        for name in reversed(workflow.topological_order):
            stage = workflow.stage(name)
            node = state["nodes"][name]
            if node["status"] not in SUCCESS_STATES or not stage.rollback_handler:
                continue
            try:
                await asyncio.wait_for(
                    self.handlers[stage.rollback_handler](
                        {"context": state["context"], "output": node["output"], "cause": cause}
                    ),
                    timeout=self.action_timeout_seconds,
                )
                await self.store.set_node(
                    run_id, name, "rolled_back", actor, "stage.rolled_back",
                    {"rollback_handler": stage.rollback_handler, "cause": cause},
                )
            except Exception as exc:
                rollback_failed = True
                logger.exception("Workflow rollback failed run_id=%s stage=%s", run_id, name)
                await self.store.set_node(
                    run_id, name, "rollback_failed", actor, "stage.rollback_failed",
                    {"error": f"{type(exc).__name__}: {exc}"},
                )
        await self.store.set_run(
            run_id,
            "failed",
            actor,
            "workflow.rollback_finished",
            {"cause": cause, "rollback_failed": rollback_failed},
            finished=True,
        )

    def _validate_handlers(self, workflow: WorkflowDefinition) -> None:
        required = {
            handler
            for stage in workflow.stages
            for handler in (stage.handler, stage.fallback_handler, stage.rollback_handler)
            if handler
        }
        missing = required - self.handlers.keys()
        if missing:
            raise WorkflowError(f"No allowlisted handler registered for: {sorted(missing)}")

    @staticmethod
    def _assert_run_definition(
        workflow: WorkflowDefinition, state: Mapping[str, Any]
    ) -> None:
        if state["workflow_name"] != workflow.name or state["workflow_version"] != workflow.version:
            raise WorkflowError("Run workflow name/version does not match this definition")

    @staticmethod
    def _enforce_policy(stage: Stage, context: Mapping[str, Any]) -> None:
        missing = REQUIRED_POLICIES - stage.policy_tags
        if missing:
            raise PolicyViolation(f"Stage {stage.name!r} lacks policy controls {sorted(missing)}")
        if stage.high_impact and not stage.approval_required:
            raise PolicyViolation(f"High-impact stage {stage.name!r} is missing human approval")
        if context.get("change_control") == "frozen":
            raise PolicyViolation("Change execution is blocked by the active change freeze")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    raise TypeError(f"Value of type {type(value).__name__} is not JSON serializable")


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _node_dict(node: sqlite3.Row) -> dict[str, Any]:
    result = dict(node)
    result["output"] = json.loads(result["output_json"]) if result["output_json"] else None
    result["approval"] = json.loads(result["approval_json"]) if result["approval_json"] else None
    del result["output_json"]
    del result["approval_json"]
    return result
