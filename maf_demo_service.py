"""
MAF Demo Coordination Service (v2 — Service as the Simulated Azure)
====================================================================

In this architecture the FastAPI service plays the role of Azure Monitor +
AKS control plane + the simulated cluster itself. The notebook's agent
tools are pure HTTP clients against this service; the dashboard polls
the same state.

Run:
    pip install "fastapi>=0.115" "uvicorn[standard]>=0.32" "pydantic>=2.7"
    uvicorn maf_demo_service:app --host 0.0.0.0 --port 8765

Endpoints — three audiences:

  Dashboard / admin
    GET  /health
    GET  /state                              cluster snapshot + timeline
    POST /state                              patch state (partial)
    POST /state/scenario                     force {normal,degraded,recovering,recovered}
    POST /state/reset                        clean reset
    POST /timeline                           append timeline event

  Agent tools (Diagnostician, Verifier — read)
    GET  /telemetry/app-insights?service=&metric=
    GET  /telemetry/sli-recovery?service=&sli=
    GET  /aks/pods?deployment=
    GET  /aks/deployments/recent?minutes=

  Agent tools (Remediator — mutate, Communicator — outbound)
    POST /aks/rollback                       body: {deployment, reason}
    POST /aks/scale                          body: {deployment, target_replicas, reason}
    POST /external/status-page               body: {severity, title, body}
    POST /external/teams                     body: {text}

  HITL bridge (notebook ↔ dashboard)
    GET  /hitl/pending
    GET  /hitl/{id}
    POST /hitl                               body: HitlRequest
    POST /hitl/{id}/decision                 body: {decision, decided_by}

State is in-memory; restart the process to reset the demo.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# App + CORS
# ---------------------------------------------------------------------------

app = FastAPI(title="MAF Demo Coordination Service", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Cluster state (moved out of the notebook)
# ---------------------------------------------------------------------------

def _initial_state() -> dict[str, Any]:
    return {
        "scenario": "normal",  # normal | degraded | recovering | recovered
        "deployment_versions": {"payment-service": "v2.4.0"},
        "previous_versions":   {"payment-service": "v2.3.9"},
        "replicas":             {"payment-service": 5, "checkout-api": 8},
        "incident_id": None,
        "incident_started_at": None,
        "incident_resolved_at": None,
        "action_log": [],
        "updated_at": _now_iso(),
        "timeline": [],
    }


STATE: dict[str, Any] = _initial_state()
PENDING_HITL: dict[str, dict[str, Any]] = {}
STATE_LOCK = asyncio.Lock()
STARTED_AT = time.time()


# ---------------------------------------------------------------------------
# SLI lookup table (moved out of the notebook)
# ---------------------------------------------------------------------------

def _slis_for_scenario(scenario: str) -> dict[str, Any]:
    """Return the synthetic SLI baseline for the current scenario step."""
    if scenario == "degraded":
        return {
            "payment_service_p95_ms": 8042,
            "payment_service_5xx_rate": 0.352,
            "payment_service_pods_crashloop": 3,
            "payment_service_pods_running": 2,
            "payment_service_exception_count": 482,
            "checkout_api_p95_ms": 950,
            "checkout_api_5xx_rate": 0.121,
            "checkout_api_exception_count": 91,
            "cart_abandonment_rate": 0.094,
        }
    if scenario == "recovering":
        return {
            "payment_service_p95_ms": 1100,
            "payment_service_5xx_rate": 0.041,
            "payment_service_pods_crashloop": 0,
            "payment_service_pods_running": 5,
            "payment_service_exception_count": 12,
            "checkout_api_p95_ms": 320,
            "checkout_api_5xx_rate": 0.018,
            "checkout_api_exception_count": 4,
            "cart_abandonment_rate": 0.031,
        }
    # normal / recovered → healthy baseline
    return {
        "payment_service_p95_ms": 215 if scenario == "normal" else 218,
        "payment_service_5xx_rate": 0.002 if scenario == "normal" else 0.0022,
        "payment_service_pods_crashloop": 0,
        "payment_service_pods_running": 5,
        "payment_service_exception_count": 3,
        "checkout_api_p95_ms": 180,
        "checkout_api_5xx_rate": 0.004,
        "checkout_api_exception_count": 2,
        "cart_abandonment_rate": 0.021,
    }


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class StatePatch(BaseModel):
    scenario: str | None = None
    deployment_versions: dict[str, str] | None = None
    previous_versions: dict[str, str] | None = None
    replicas: dict[str, int] | None = None
    incident_id: str | None = None
    incident_started_at: str | None = None
    incident_resolved_at: str | None = None


class ScenarioUpdate(BaseModel):
    scenario: str


class TimelineEvent(BaseModel):
    t: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime("%H:%M:%S"))
    src: str
    msg: str


class HitlRequest(BaseModel):
    tool: str
    args: dict[str, Any]
    evidence: list[str] = []
    blast_radius: str | None = None
    confidence: str | None = None
    risk: str = "high"
    requested_by: str = "Remediator"
    from_version: str | None = None
    to_version: str | None = None


class HitlDecision(BaseModel):
    decision: str
    decided_by: str | None = "lab-operator"


class RollbackRequest(BaseModel):
    deployment: str
    reason: str


class ScaleRequest(BaseModel):
    deployment: str
    target_replicas: int
    reason: str


class StatusPageUpdate(BaseModel):
    severity: str  # investigating | identified | monitoring | resolved
    title: str
    body: str


class TeamsMessage(BaseModel):
    text: str


# ---------------------------------------------------------------------------
# Health + State (dashboard/admin)
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "maf-demo-coordination",
        "version": "2.0.0",
        "uptime_s": int(time.time() - STARTED_AT),
        "pending_hitl_count": sum(1 for v in PENDING_HITL.values() if v.get("decision") is None),
    }


@app.get("/state")
async def get_state() -> dict[str, Any]:
    async with STATE_LOCK:
        return dict(STATE)


@app.post("/state")
async def patch_state(patch: StatePatch) -> dict[str, Any]:
    async with STATE_LOCK:
        for k, v in patch.model_dump(exclude_none=True).items():
            STATE[k] = v
        STATE["updated_at"] = _now_iso()
        return dict(STATE)


@app.post("/state/scenario")
async def set_scenario(payload: ScenarioUpdate) -> dict[str, Any]:
    valid = {"normal", "degraded", "recovering", "recovered"}
    if payload.scenario not in valid:
        raise HTTPException(400, f"scenario must be one of {sorted(valid)}")
    async with STATE_LOCK:
        STATE["scenario"] = payload.scenario
        # Flip deployment version to match the scenario (only when version is at
        # its "other" stable value, so the failure-injection path can post
        # scenario=degraded via PATCH /state without re-corrupting the version).
        if payload.scenario == "degraded":
            if STATE["deployment_versions"].get("payment-service") == "v2.4.0":
                STATE["deployment_versions"]["payment-service"] = "v2.4.1"
                STATE["previous_versions"]["payment-service"] = "v2.4.0"
            if not STATE.get("incident_id"):
                STATE["incident_id"] = "INC-" + datetime.now(timezone.utc).strftime("%m%d-%H%M")
                STATE["incident_started_at"] = _now_iso()
        elif payload.scenario in ("normal", "recovering", "recovered"):
            if STATE["deployment_versions"].get("payment-service") == "v2.4.1":
                STATE["deployment_versions"]["payment-service"] = "v2.4.0"
        STATE["updated_at"] = _now_iso()
        return dict(STATE)


@app.post("/state/reset")
async def reset_state() -> dict[str, Any]:
    async with STATE_LOCK:
        global STATE
        STATE = _initial_state()
        PENDING_HITL.clear()
        PUBLISHED_UPDATES.clear()
        return dict(STATE)


# ---------------------------------------------------------------------------
# Telemetry — read-only (Diagnostician, Verifier)
# ---------------------------------------------------------------------------

@app.get("/telemetry/app-insights")
async def telemetry_app_insights(
    service: str = Query(...),
    metric: str = Query(...),
) -> dict[str, Any]:
    """Mock Azure Application Insights query.
    Same return shape the notebook's query_app_insights tool produced."""
    async with STATE_LOCK:
        scenario = STATE["scenario"]
    slis = _slis_for_scenario(scenario)
    mapping = {
        ("payment-service", "latency_p95"):     slis["payment_service_p95_ms"],
        ("payment-service", "error_rate_5xx"):  slis["payment_service_5xx_rate"],
        ("payment-service", "exception_count"): slis["payment_service_exception_count"],
        ("checkout-api",    "latency_p95"):     slis["checkout_api_p95_ms"],
        ("checkout-api",    "error_rate_5xx"):  slis["checkout_api_5xx_rate"],
        ("checkout-api",    "exception_count"): slis["checkout_api_exception_count"],
    }
    value = mapping.get((service, metric), "metric_not_found")
    return {
        "service": service,
        "metric": metric,
        "value": value,
        "window": "last_5_min",
        "scenario_context": scenario,
    }


@app.get("/telemetry/sli-recovery")
async def telemetry_sli_recovery(
    service: str = Query(...),
    sli: str = Query(...),
) -> dict[str, Any]:
    """Check whether an SLI is within nominal bounds.
    Same return shape as the notebook's check_sli_recovery tool."""
    async with STATE_LOCK:
        scenario = STATE["scenario"]
    slis = _slis_for_scenario(scenario)
    # Normalise the service name — LLMs frequently emit 'payment_service'
    # (underscore, matching the metric key names) instead of the canonical
    # hyphenated form. Treat both as the same service.
    service_norm = (service or "").strip().lower().replace("_", "-")
    if service_norm == "payment-service":
        if sli == "latency_p95":
            v = slis["payment_service_p95_ms"]
            return {"sli": sli, "value_ms": v, "within_bounds": v < 1000}
        if sli == "error_rate_5xx":
            v = slis["payment_service_5xx_rate"]
            return {"sli": sli, "rate": v, "within_bounds": v < 0.02}
        if sli == "pod_health":
            crash = slis["payment_service_pods_crashloop"]
            return {"sli": sli, "crashloop_pods": crash, "within_bounds": crash == 0}
    return {"sli": sli, "value": None, "within_bounds": False, "note": "unknown service/sli"}


# ---------------------------------------------------------------------------
# AKS — read (Diagnostician)
# ---------------------------------------------------------------------------

@app.get("/aks/pods")
async def aks_pods(deployment: str = Query(...)) -> dict[str, Any]:
    """Mock kubectl get pods. Same return shape as get_aks_pod_status tool."""
    async with STATE_LOCK:
        scenario = STATE["scenario"]
        version = STATE["deployment_versions"].get(deployment, "unknown")
        replicas = STATE["replicas"].get(deployment, 0)
    slis = _slis_for_scenario(scenario)
    if deployment == "payment-service":
        return {
            "deployment": deployment,
            "running": slis["payment_service_pods_running"],
            "crashloopbackoff": slis["payment_service_pods_crashloop"],
            "pending": 0,
            "current_version": version,
        }
    return {
        "deployment": deployment,
        "running": replicas,
        "crashloopbackoff": 0,
        "pending": 0,
        "current_version": version,
    }


@app.get("/aks/deployments/recent")
async def aks_recent_deployments(minutes: int = Query(60)) -> dict[str, Any]:
    """Mock deployment-history query. Same return shape as get_recent_deployments tool."""
    async with STATE_LOCK:
        version = STATE["deployment_versions"].get("payment-service", "v2.4.0")
        scenario = STATE["scenario"]
    if scenario == "degraded" or version == "v2.4.1":
        return {
            "deployments": [
                {
                    "deployment": "payment-service",
                    "from_version": "v2.4.0",
                    "to_version": "v2.4.1",
                    "deployed_at": "18 minutes ago",
                    "deployed_by": "ci-cd-pipeline",
                    "change_notes": "Performance optimization: connection pooling refactor",
                }
            ]
        }
    return {"deployments": []}


# ---------------------------------------------------------------------------
# AKS — mutate (Remediator). These advance the scenario state machine.
# ---------------------------------------------------------------------------

@app.post("/aks/rollback")
async def aks_rollback(req: RollbackRequest) -> dict[str, Any]:
    """Roll back a deployment to its previous version."""
    async with STATE_LOCK:
        if req.deployment not in STATE["deployment_versions"]:
            return {"status": "error", "message": f"Unknown deployment: {req.deployment}"}
        prev = STATE["previous_versions"].get(req.deployment)
        if not prev:
            return {"status": "error", "message": "No previous version available"}

        current = STATE["deployment_versions"][req.deployment]
        STATE["deployment_versions"][req.deployment] = prev
        STATE["action_log"].append({
            "action": "rollback", "deployment": req.deployment,
            "from": current, "to": prev, "reason": req.reason, "at": _now_iso(),
        })
        if STATE["scenario"] == "degraded" and req.deployment == "payment-service":
            STATE["scenario"] = "recovering"
            # Model SLI stabilisation: after a short delay flip to 'recovered'
            # so the dashboard pill, the Verifier's SLI check, and the
            # Communicator's 'resolved' severity all line up.
            asyncio.create_task(_advance_to_recovered(delay_s=5.0))
        STATE["updated_at"] = _now_iso()
        return {"status": "success", "deployment": req.deployment, "now_running": prev}


async def _advance_to_recovered(delay_s: float) -> None:
    """Background task: after `delay_s` seconds, flip scenario
    'recovering' -> 'recovered' if it hasn't moved elsewhere in the meantime."""
    await asyncio.sleep(delay_s)
    async with STATE_LOCK:
        if STATE["scenario"] == "recovering":
            STATE["scenario"] = "recovered"
            STATE["incident_resolved_at"] = _now_iso()
            STATE["updated_at"] = _now_iso()


@app.post("/aks/scale")
async def aks_scale(req: ScaleRequest) -> dict[str, Any]:
    """Scale a deployment to a target replica count."""
    if not (1 <= req.target_replicas <= 20):
        return {"status": "error", "message": "target_replicas out of allowed range 1-20"}
    async with STATE_LOCK:
        STATE["replicas"][req.deployment] = req.target_replicas
        STATE["action_log"].append({
            "action": "scale", "deployment": req.deployment,
            "to": req.target_replicas, "reason": req.reason, "at": _now_iso(),
        })
        STATE["updated_at"] = _now_iso()
        return {"status": "success", "deployment": req.deployment, "replicas": req.target_replicas}


# ---------------------------------------------------------------------------
# External systems — outbound (Communicator)
# ---------------------------------------------------------------------------

PUBLISHED_UPDATES: list[dict[str, Any]] = []


@app.post("/external/status-page")
async def external_status_page(req: StatusPageUpdate) -> dict[str, Any]:
    """Mock customer-facing status page publish."""
    entry = {
        "channel": "status_page",
        "severity": req.severity,
        "title": req.title[:80],
        "body": req.body,
        "ts": _now_iso(),
    }
    PUBLISHED_UPDATES.append(entry)
    return {"status": "published", "id": uuid.uuid4().hex[:8]}


@app.post("/external/teams")
async def external_teams(req: TeamsMessage) -> dict[str, Any]:
    """Mock internal Teams channel post."""
    entry = {"channel": "teams_incidents", "text": req.text, "ts": _now_iso()}
    PUBLISHED_UPDATES.append(entry)
    return {"status": "posted"}


@app.get("/external/published")
async def list_published() -> list[dict[str, Any]]:
    """Inspector endpoint: list everything the Communicator has posted."""
    return list(PUBLISHED_UPDATES)


# ---------------------------------------------------------------------------
# Timeline
# ---------------------------------------------------------------------------

@app.post("/timeline")
async def post_timeline(event: TimelineEvent) -> dict[str, Any]:
    async with STATE_LOCK:
        STATE["timeline"].append(event.model_dump())
        if len(STATE["timeline"]) > 500:
            STATE["timeline"] = STATE["timeline"][-500:]
        STATE["updated_at"] = _now_iso()
        return {"ok": True, "count": len(STATE["timeline"])}


# ---------------------------------------------------------------------------
# HITL bridge (notebook ↔ dashboard)
# ---------------------------------------------------------------------------

@app.get("/hitl/pending")
async def list_pending() -> list[dict[str, Any]]:
    async with STATE_LOCK:
        return [dict(v) for v in PENDING_HITL.values() if v.get("decision") is None]


@app.get("/hitl/{approval_id}")
async def get_approval(approval_id: str) -> dict[str, Any]:
    async with STATE_LOCK:
        if approval_id not in PENDING_HITL:
            raise HTTPException(404, f"Approval {approval_id!r} not found")
        return dict(PENDING_HITL[approval_id])


@app.post("/hitl")
async def create_approval(req: HitlRequest) -> dict[str, Any]:
    approval_id = "apr_" + uuid.uuid4().hex[:6]
    async with STATE_LOCK:
        PENDING_HITL[approval_id] = {
            "id": approval_id,
            **req.model_dump(),
            "created_at": _now_iso(),
            "decision": None,
            "decided_at": None,
            "decided_by": None,
        }
        return dict(PENDING_HITL[approval_id])


@app.post("/hitl/{approval_id}/decision")
async def submit_decision(approval_id: str, payload: HitlDecision) -> dict[str, Any]:
    if payload.decision not in ("approve", "reject"):
        raise HTTPException(400, "decision must be 'approve' or 'reject'")
    async with STATE_LOCK:
        if approval_id not in PENDING_HITL:
            raise HTTPException(404, f"Approval {approval_id!r} not found")
        if PENDING_HITL[approval_id].get("decision") is not None:
            raise HTTPException(409, "Approval already decided")
        PENDING_HITL[approval_id]["decision"] = payload.decision
        PENDING_HITL[approval_id]["decided_by"] = payload.decided_by
        PENDING_HITL[approval_id]["decided_at"] = _now_iso()
        return dict(PENDING_HITL[approval_id])


# ---------------------------------------------------------------------------
# Convenience: run as a script
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765)
