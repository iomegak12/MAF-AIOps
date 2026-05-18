# MAF-AIOps: Self-Healing Agents Lab

A hands-on demo of an **AIOps self-healing system** built on the **Microsoft Agent Framework (MAF)**. It coordinates multiple specialized agents (Diagnostician, Remediator, Verifier, Communicator) against a simulated AKS + Azure Monitor environment, with a Human-in-the-Loop (HITL) bridge and a live dashboard.

## Architecture

- **`maf_demo_service.py`** — FastAPI service that simulates Azure Monitor, the AKS control plane, and the cluster state. Exposes endpoints for telemetry, remediation actions, and HITL approvals.
- **`MAF_SelfHealing_Agents_Lab.ipynb`** — Jupyter notebook containing the multi-agent workflow. Agents call the service as HTTP tools.
- **`MAF_SelfHealing_Dashboard.html`** — Live dashboard that polls service state, displays the timeline, and approves/rejects pending HITL requests.

State is in-memory — restart the service to reset the demo.

## Requirements

- Python 3.12+
- Dependencies declared in [pyproject.toml](pyproject.toml):
  - `agent-framework>=1.4.0`
  - `fastapi>=0.115`
  - `uvicorn[standard]>=0.32`
  - `pydantic>=2.7`
  - `ipykernel`, `nest-asyncio`

## Setup

```powershell
# Create and activate a virtual environment (recommended)
python -m venv .venv
.\.venv\Scripts\activate

# Install dependencies
pip install -e .
```

## Running

1. **Start the coordination service:**

   ```powershell
   .\start.cmd
   ```

   Or directly:

   ```powershell
   uvicorn maf_demo_service:app --host 0.0.0.0 --port 8765
   ```

2. **Open the dashboard:** open [MAF_SelfHealing_Dashboard.html](MAF_SelfHealing_Dashboard.html) in a browser.

3. **Run the lab:** open [MAF_SelfHealing_Agents_Lab.ipynb](MAF_SelfHealing_Agents_Lab.ipynb) in VS Code or Jupyter and execute the cells.

## Service Endpoints (overview)

| Audience | Endpoint | Purpose |
|---|---|---|
| Dashboard | `GET /state`, `POST /state/scenario`, `POST /state/reset` | Cluster snapshot, scenario control |
| Agents (read) | `GET /telemetry/app-insights`, `GET /aks/pods`, `GET /aks/deployments/recent` | Diagnostics |
| Agents (mutate) | `POST /aks/rollback`, `POST /aks/scale` | Remediation |
| Agents (outbound) | `POST /external/status-page`, `POST /external/teams` | Communication |
| HITL bridge | `GET /hitl/pending`, `POST /hitl/{id}/decision` | Human approvals |

See the docstring at the top of [maf_demo_service.py](maf_demo_service.py) for full details.

## License

Released under the [MIT License](LICENSE).
