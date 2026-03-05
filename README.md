# Hospital Management System

Role-based Hospital Management System built with Flask + SQLite + vanilla frontend.

## Features

- Multi-portal login:
  - `Admin`
  - `Clinical` (Doctor + Nurse)
  - `Front Desk` (Receptionist)
- Role-based access control on backend routes and frontend sections.
- Core modules:
  - Patients, doctors, slots, appointments
  - Medical records, prescriptions, lab tests
  - Billing and reminders
  - Audit logs
- Nurse workflows:
  - Triage queue
  - Nursing notes (vitals + notes)
  - Nurse tasks
- Admin analytics:
  - Real-time chart refresh (every 10 seconds)
  - Daily appointments, daily revenue
  - Appointment status mix
  - Triage priority mix
  - Top doctor workload

## Tech Stack

- Backend: Flask
- Database: SQLite
- Frontend: HTML/CSS/JavaScript
- Data generation: pandas scripts

## Project Structure

```text
backend/
  database.py
  routes.py
frontend/
  index.html
  css/style.css
  js/app.js
scripts/
  db_stress_test.py
  seed_demo_data_pandas.py
  smart_seed_ai_pandas.py
main.py
requirements.txt
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

Open:

- App: `http://localhost:5000`
- API base: `http://localhost:5000/api`

## Default Accounts

- `admin / admin123`
- `doctor1 / doctor123`
- `nurse1 / nurse123`
- `reception1 / reception123`

## Data Generation

### 1) Quick realistic seeding (pandas)

```bash
./.venv/bin/python scripts/seed_demo_data_pandas.py --count 500
```

### 2) Smart AI-like realistic seeding (chunked big-data capable)

`smart_seed_ai_pandas.py` uses a small heuristic planner (`MicroAIPlanner`) to model realistic behavior:
- risk scoring
- status outcomes (scheduled/completed/cancelled)
- billing/payment tendency
- triage and nursing activity patterns

Example:

```bash
./.venv/bin/python scripts/smart_seed_ai_pandas.py \
  --patients 50000 \
  --doctors 2000 \
  --users 20000 \
  --appointments 200000 \
  --triage 20000 \
  --nursing-notes 30000 \
  --nurse-tasks 25000 \
  --batch-size 20000 \
  --seed 42
```

For very large runs:

```bash
./.venv/bin/python scripts/smart_seed_ai_pandas.py ... --allow-huge
```

## Admin Analytics API

Admin-only endpoint:

```text
GET /api/analytics?days=14
```

Returns:
- `daily_appointments`
- `daily_revenue`
- `appointment_status`
- `triage_priority`
- `top_doctor_workload`

## Notes

- SQLite is suitable for local/dev and moderate workloads.
- For production/high-concurrency workloads, migrate to PostgreSQL or MySQL.
- `*.db` files are gitignored by default in this project.
