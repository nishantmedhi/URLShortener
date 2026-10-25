from collections.abc import Mapping
from typing import Any

from orchestrator.engine import Stage, WorkflowDefinition


SCENARIOS = {
    "greenfield": {
        "description": "Create a new URL shortener capability from normalized requirements.",
        "validation": "Entry gate requires a request; tests and documentation must synchronize before review.",
    },
    "brownfield": {
        "description": "Change an existing URL shortener without breaking established API behavior.",
        "validation": "Entry gate requires affected modules; tests and documentation must synchronize before review.",
    },
    "ambiguous": {
        "description": "Resolve conflicting or incomplete requirements by recording explicit assumptions.",
        "validation": "Entry gate rejects a request until at least one assumption is recorded.",
    },
}


def gate(context: Mapping[str, Any]) -> tuple[bool, str]:
    request = context.get("request")
    if not isinstance(request, str) or not request.strip():
        return False, "A non-empty change request is required"
    scenario = context.get("scenario", "greenfield")
    if scenario not in SCENARIOS:
        return False, "Unknown engineering scenario"
    if scenario == "brownfield" and not context.get("affected_modules"):
        return False, "Brownfield changes must identify at least one affected module"
    if scenario == "ambiguous" and not context.get("assumptions"):
        return False, "Ambiguous requirements must record at least one explicit assumption"
    return True, f"{scenario} entry conditions and required context are present"


def build_workflow(scenario: str = "greenfield") -> WorkflowDefinition:
    if scenario not in SCENARIOS:
        raise ValueError(f"Unknown scenario {scenario!r}; choose one of {sorted(SCENARIOS)}")
    return WorkflowDefinition(
        name=f"url-shortener-sdlc-{scenario}",
        version="2",
        stages=(
            Stage("requirements", "requirements"),
            Stage("architecture", "architecture", ("requirements",)),
            Stage("implementation", "implementation", ("architecture",)),
            Stage("tests", "tests", ("implementation",)),
            Stage("documentation", "documentation", ("implementation",)),
            Stage(
                "security_approval",
                "security_approval",
                ("tests", "documentation"),
                high_impact=True,
                approval_required=True,
                rollback_handler="rollback",
                approval_group="security",
            ),
            Stage(
                "compliance_approval",
                "compliance_approval",
                ("security_approval",),
                high_impact=True,
                approval_required=True,
                rollback_handler="rollback",
                approval_group="compliance",
            ),
            Stage(
                "change_control_approval",
                "change_control_approval",
                ("compliance_approval",),
                high_impact=True,
                approval_required=True,
                rollback_handler="rollback",
                approval_group="change_control",
            ),
            Stage(
                "release",
                "release",
                ("change_control_approval", "tests", "documentation"),
                high_impact=True,
                approval_required=True,
                rollback_handler="rollback",
                approval_group="release",
            ),
        ),
        entry_gate=gate,
        exit_gate=lambda context: (bool(context.get("outputs", {}).get("release")), "release output exists"),
    )


def _demo_handler(stage_name: str):
    async def execute(inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        context = inputs["context"]
        return {
            "stage": stage_name,
            "status": "completed",
            "scenario": context.get("scenario", "greenfield"),
            "dependency_names": sorted(inputs["dependencies"]),
            "decision_context": {
                key: context[key]
                for key in ("scenario", "affected_modules", "assumptions")
                if key in context
            },
            "note": "Local demonstration handler; register real agent/tool executors for production work.",
        }

    return execute


async def release_handler(inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    return {
        "release_ready": True,
        "scenario": inputs["context"].get("scenario", "greenfield"),
        "approved_dependencies": sorted(inputs["dependencies"]),
        "change_control": inputs["context"].get("change_control"),
    }


async def rollback_handler(inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    return {"rollback_requested": True, "cause": inputs["cause"]}


def handlers():
    workflow = build_workflow()
    result = {
        stage.handler: _demo_handler(stage.name)
        for stage in workflow.stages
        if stage.handler != "release"
    }
    result["release"] = release_handler
    result["rollback"] = rollback_handler
    return result
