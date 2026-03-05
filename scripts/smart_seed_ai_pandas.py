import argparse
import hashlib
import os
import random
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.database import Database


MAX_SAFE_ENTITY = 50_000_000
MAX_SAFE_TOTAL = 200_000_000


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def next_id(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()[0])


def chunk_sizes(total: int, batch_size: int):
    done = 0
    while done < total:
        size = min(batch_size, total - done)
        yield done, size
        done += size


@dataclass
class MicroAIPlanner:
    rng: random.Random

    def risk_score(self, age: int, chronic: bool, triage_priority: str) -> float:
        pri = {"Low": 0.1, "Medium": 0.35, "High": 0.7, "Critical": 1.0}[triage_priority]
        age_factor = min(max((age - 20) / 70, 0), 1)
        chronic_factor = 0.25 if chronic else 0.0
        jitter = self.rng.uniform(-0.08, 0.08)
        return max(0.0, min(1.0, 0.25 * age_factor + chronic_factor + 0.55 * pri + jitter))

    def choose_status(self, appointment_date: date, risk: float) -> str:
        if appointment_date < date.today():
            if risk > 0.6:
                return self.rng.choices(["Completed", "Cancelled"], weights=[0.9, 0.1], k=1)[0]
            return self.rng.choices(["Completed", "Cancelled"], weights=[0.78, 0.22], k=1)[0]
        return self.rng.choices(["Scheduled", "Cancelled"], weights=[0.92, 0.08], k=1)[0]

    def invoice_components(self, risk: float):
        consult = round(35 + 110 * risk + self.rng.uniform(5, 30), 2)
        lab = round(max(0, self.rng.uniform(0, 220) * (0.4 + risk)), 2)
        pharm = round(max(0, self.rng.uniform(10, 180) * (0.5 + risk)), 2)
        total = round(consult + lab + pharm, 2)
        paid_prob = 0.55 + (0.25 if risk < 0.4 else -0.12)
        status = "Paid" if self.rng.random() < paid_prob else "Unpaid"
        return consult, lab, pharm, total, status


def insert_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame, columns):
    if df.empty:
        return 0
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
    conn.executemany(sql, df[columns].itertuples(index=False, name=None))
    return len(df)


def build_users_chunk(start_id: int, offset: int, size: int):
    roles = ["Patient", "Patient", "Patient", "Nurse", "Doctor", "Receptionist"]
    rows = []
    for idx in range(size):
        uid = start_id + offset + idx
        role = roles[(offset + idx) % len(roles)]
        rows.append(
            {
                "username": f"user{uid}",
                "password_hash": hash_password("pass123"),
                "role": role,
                "full_name": f"{role} User {uid}",
                "email": f"user{uid}@example.test",
            }
        )
    return pd.DataFrame(rows)


def build_patients_chunk(start_id: int, offset: int, size: int, rng: random.Random):
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "David", "Elizabeth", "Daniel", "Susan", "Brian", "Amina", "Grace", "Peter", "Lucy", "Kevin", "Sarah", "George"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Ngugi", "Otieno", "Kimani", "Wanjiku", "Mwangi", "Odhiambo"]
    genders = ["Male", "Female", "Other"]

    rows = []
    for idx in range(size):
        pid = start_id + offset + idx
        age = int(max(1, min(95, rng.gauss(41, 18))))
        rows.append(
            {
                "first_name": rng.choice(first_names),
                "last_name": rng.choice(last_names),
                "age": age,
                "gender": rng.choice(genders),
                "phone": f"+1{rng.randint(2000000000, 9999999999)}",
                "email": f"patient{pid}@example.test",
                "address": f"{rng.randint(10, 9999)} Wellness St",
            }
        )
    return pd.DataFrame(rows)


def build_doctors_chunk(start_id: int, offset: int, size: int, rng: random.Random):
    first_names = ["Olivia", "Noah", "Emma", "Liam", "Sophia", "Elijah", "Ava", "Mason", "Mia", "Lucas", "Charlotte", "Amelia"]
    last_names = ["Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Moore", "Taylor", "Clark", "Lewis"]
    specializations = ["General Medicine", "Cardiology", "Neurology", "Orthopedics", "Pediatrics", "Dermatology", "Oncology", "Pulmonology"]

    rows = []
    for idx in range(size):
        did = start_id + offset + idx
        rows.append(
            {
                "first_name": rng.choice(first_names),
                "last_name": rng.choice(last_names),
                "specialization": specializations[did % len(specializations)],
                "phone": f"+1{rng.randint(2000000000, 9999999999)}",
                "email": f"doctor{did}@example.test",
                "available_days": "Mon-Fri 08:00-17:00",
            }
        )
    return pd.DataFrame(rows)


def build_clinical_chunk(planner: MicroAIPlanner, patient_min: int, patient_max: int, doctor_min: int, doctor_max: int, nurse_ids, size: int):
    rng = planner.rng
    today = date.today()
    specializations = ["General Medicine", "Cardiology", "Neurology", "Orthopedics", "Pediatrics", "Dermatology", "Oncology", "Pulmonology"]
    reason_by_spec = {
        "General Medicine": ["Fever", "Headache", "Fatigue", "General checkup"],
        "Cardiology": ["Chest pain", "Palpitations", "Hypertension follow-up"],
        "Neurology": ["Migraine", "Dizziness", "Numbness"],
        "Orthopedics": ["Joint pain", "Back pain", "Fracture follow-up"],
        "Pediatrics": ["Child fever", "Vaccination", "Growth review"],
        "Dermatology": ["Rash", "Skin irritation", "Acne follow-up"],
        "Oncology": ["Therapy review", "Pain management", "Lab follow-up"],
        "Pulmonology": ["Cough", "Breathing difficulty", "Asthma follow-up"],
    }
    triage_levels = ["Low", "Medium", "High", "Critical"]

    appointments = []
    records = []
    prescriptions = []
    labs = []
    invoices = []
    reminders = []
    triage = []
    nursing_notes = []
    nurse_tasks = []

    for _ in range(size):
        patient_id = rng.randint(patient_min, patient_max)
        doctor_id = rng.randint(doctor_min, doctor_max)

        age = int(max(1, min(95, rng.gauss(41, 18))))
        chronic = bool(rng.random() < (0.18 if age < 45 else 0.42))
        triage_priority = rng.choices(triage_levels, weights=[0.46, 0.3, 0.18, 0.06], k=1)[0]
        risk = planner.risk_score(age, chronic, triage_priority)

        day_offset = rng.randint(-45, 20)
        appt_day = today + timedelta(days=day_offset)
        if appt_day.weekday() >= 5 and rng.random() < 0.7:
            appt_day += timedelta(days=(7 - appt_day.weekday()))

        hour = rng.choice([8, 9, 10, 11, 13, 14, 15, 16, 17])
        minute = rng.choice([0, 30])

        spec = specializations[doctor_id % len(specializations)]
        reason = rng.choice(reason_by_spec.get(spec, ["Consultation"]))
        status = planner.choose_status(appt_day, risk)

        appointments.append(
            {
                "patient_id": patient_id,
                "doctor_id": doctor_id,
                "appointment_date": appt_day.isoformat(),
                "appointment_time": f"{hour:02d}:{minute:02d}:00",
                "reason": reason,
                "status": status,
            }
        )

        if rng.random() < (0.68 if status == "Completed" else 0.2):
            records.append(
                {
                    "patient_id": patient_id,
                    "doctor_id": doctor_id,
                    "diagnosis": f"{reason} - assessed",
                    "prescription": "Standard treatment plan",
                    "notes": f"Risk score {risk:.2f}. Follow-up advised.",
                    "visit_date": appt_day.isoformat(),
                }
            )

        if rng.random() < (0.6 if risk > 0.4 else 0.35):
            prescriptions.append(
                {
                    "patient_id": patient_id,
                    "doctor_id": doctor_id,
                    "medication_name": rng.choice(["Amoxicillin", "Ibuprofen", "Metformin", "Losartan", "Paracetamol", "Omeprazole"]),
                    "dosage": rng.choice(["250mg", "500mg", "10mg", "20mg"]),
                    "frequency": rng.choice(["Once daily", "Twice daily", "Every 8 hours"]),
                    "duration_days": rng.choice([3, 5, 7, 10, 14, 30]),
                    "notes": "Take with water",
                }
            )

        if rng.random() < (0.72 if risk > 0.55 else 0.38):
            completed = appt_day < today and rng.random() < 0.7
            labs.append(
                {
                    "patient_id": patient_id,
                    "doctor_id": doctor_id,
                    "test_name": rng.choice(["CBC", "Lipid Panel", "LFT", "KFT", "HbA1c", "X-Ray"]),
                    "status": "Completed" if completed else "Ordered",
                    "result_text": "Within expected range" if completed and rng.random() < 0.6 else None,
                    "completed_at": f"{appt_day.isoformat()} 12:00:00" if completed else None,
                }
            )

        consult, lab_fee, pharm, total, pay_status = planner.invoice_components(risk)
        invoices.append(
            {
                "patient_id": patient_id,
                "consultation_fee": consult,
                "lab_fee": lab_fee,
                "pharmacy_fee": pharm,
                "total_amount": total,
                "status": pay_status,
            }
        )

        if status == "Scheduled" and appt_day >= today and rng.random() < 0.7:
            reminders.append(
                {
                    "patient_id": patient_id,
                    "channel": rng.choice(["SMS", "Email"]),
                    "message": f"Reminder: appointment on {appt_day.isoformat()}",
                    "status": "Queued",
                    "scheduled_for": f"{(appt_day - timedelta(days=1)).isoformat()} 10:00:00",
                }
            )

        if rng.random() < 0.35:
            triage.append(
                {
                    "patient_id": patient_id,
                    "priority": triage_priority,
                    "symptoms": rng.choice(["Fever", "Shortness of breath", "Chest discomfort", "Dizziness", "Injury", "Abdominal pain"]),
                    "status": rng.choices(["Waiting", "In Progress", "Completed"], weights=[0.5, 0.25, 0.25], k=1)[0],
                    "assigned_nurse_id": rng.choice(nurse_ids) if nurse_ids else None,
                }
            )

        if rng.random() < 0.45:
            nursing_notes.append(
                {
                    "patient_id": patient_id,
                    "nurse_id": rng.choice(nurse_ids) if nurse_ids else 1,
                    "blood_pressure": f"{rng.randint(95, 150)}/{rng.randint(60, 95)}",
                    "temperature": round(rng.uniform(36.0, 39.1), 1),
                    "pulse": rng.randint(55, 120),
                    "respiratory_rate": rng.randint(12, 24),
                    "oxygen_saturation": rng.randint(88, 100),
                    "note": rng.choice(["Patient stable", "Monitor closely", "Mild distress", "Responding to treatment"]),
                }
            )

        if rng.random() < 0.35:
            due = datetime.now() + timedelta(hours=rng.randint(1, 72))
            nurse_tasks.append(
                {
                    "patient_id": patient_id,
                    "assigned_nurse_id": rng.choice(nurse_ids) if nurse_ids else 1,
                    "task_title": rng.choice(["Recheck vitals", "Medication administration", "Wound dressing", "Prep patient", "Monitor intake"]),
                    "task_details": "Generated by micro-ai workload planner",
                    "due_at": due.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": rng.choices(["Pending", "In Progress", "Done"], weights=[0.58, 0.24, 0.18], k=1)[0],
                }
            )

    return {
        "appointments": pd.DataFrame(appointments),
        "medical_records": pd.DataFrame(records),
        "prescriptions": pd.DataFrame(prescriptions),
        "lab_tests": pd.DataFrame(labs),
        "invoices": pd.DataFrame(invoices),
        "reminders": pd.DataFrame(reminders),
        "triage_queue": pd.DataFrame(triage),
        "nursing_notes": pd.DataFrame(nursing_notes),
        "nurse_tasks": pd.DataFrame(nurse_tasks),
    }


def main():
    parser = argparse.ArgumentParser(description="Smart realistic test data generator (pandas + micro-ai heuristics, chunked for big data).")
    parser.add_argument("--db-path", default="hospital.db")
    parser.add_argument("--patients", type=int, default=500)
    parser.add_argument("--doctors", type=int, default=80)
    parser.add_argument("--users", type=int, default=300)
    parser.add_argument("--appointments", type=int, default=2000)
    parser.add_argument("--triage", type=int, default=0, help="Additional standalone triage rows")
    parser.add_argument("--nursing-notes", type=int, default=0, help="Additional standalone nursing notes rows")
    parser.add_argument("--nurse-tasks", type=int, default=0, help="Additional standalone nurse tasks rows")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batch-size", type=int, default=20000)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--allow-huge", action="store_true")
    args = parser.parse_args()

    if args.batch_size < 1000:
        raise SystemExit("--batch-size too small. Use at least 1000 for efficient generation.")

    entity_counts = {
        "patients": args.patients,
        "doctors": args.doctors,
        "users": args.users,
        "appointments": args.appointments,
        "triage": args.triage,
        "nursing-notes": args.nursing_notes,
        "nurse-tasks": args.nurse_tasks,
    }
    total_requested = sum(entity_counts.values())

    if not args.allow_huge:
        too_large = [f"{k}={v:,}" for k, v in entity_counts.items() if v > MAX_SAFE_ENTITY]
        if too_large:
            raise SystemExit(
                "Requested values exceed safe per-entity defaults: "
                + ", ".join(too_large)
                + ". Re-run with --allow-huge to force generation."
            )
        if total_requested > MAX_SAFE_TOTAL:
            raise SystemExit(
                f"Requested total rows ({total_requested:,}) exceed safe total ({MAX_SAFE_TOTAL:,}). "
                "Re-run with --allow-huge to force generation."
            )

    if args.reset and os.path.exists(args.db_path):
        os.remove(args.db_path)

    Database(db_name=args.db_path)

    rng = random.Random(args.seed)
    planner = MicroAIPlanner(rng)

    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    user_start = next_id(conn, "users")
    patient_start = next_id(conn, "patients")
    doctor_start = next_id(conn, "doctors")

    # Insert users in chunks
    for off, size in chunk_sizes(args.users, args.batch_size):
        conn.execute("BEGIN")
        users_df = build_users_chunk(user_start, off, size)
        insert_df(conn, "users", users_df, ["username", "password_hash", "role", "full_name", "email"])
        conn.commit()

    # Insert patients in chunks
    for off, size in chunk_sizes(args.patients, args.batch_size):
        conn.execute("BEGIN")
        patients_df = build_patients_chunk(patient_start, off, size, rng)
        insert_df(conn, "patients", patients_df, ["first_name", "last_name", "age", "gender", "phone", "email", "address"])
        conn.commit()

    # Insert doctors in chunks
    for off, size in chunk_sizes(args.doctors, args.batch_size):
        conn.execute("BEGIN")
        doctors_df = build_doctors_chunk(doctor_start, off, size, rng)
        insert_df(conn, "doctors", doctors_df, ["first_name", "last_name", "specialization", "phone", "email", "available_days"])
        conn.commit()

    nurse_user_ids = [
        row[0]
        for row in conn.execute("SELECT id FROM users WHERE role = 'Nurse' ORDER BY id").fetchall()
    ]

    patient_min, patient_max = patient_start, patient_start + args.patients - 1
    doctor_min, doctor_max = doctor_start, doctor_start + args.doctors - 1

    if args.patients == 0 or args.doctors == 0:
        raise SystemExit("patients and doctors must be > 0 to generate clinical data.")

    inserted = {
        "appointments": 0,
        "medical_records": 0,
        "prescriptions": 0,
        "lab_tests": 0,
        "invoices": 0,
        "reminders": 0,
        "triage_queue": 0,
        "nursing_notes": 0,
        "nurse_tasks": 0,
    }

    # Generate appointments and related clinical/billing/reminder data in streaming chunks.
    for off, size in chunk_sizes(args.appointments, args.batch_size):
        _ = off
        chunk = build_clinical_chunk(planner, patient_min, patient_max, doctor_min, doctor_max, nurse_user_ids, size)
        conn.execute("BEGIN")
        inserted["appointments"] += insert_df(conn, "appointments", chunk["appointments"], ["patient_id", "doctor_id", "appointment_date", "appointment_time", "reason", "status"])
        inserted["medical_records"] += insert_df(conn, "medical_records", chunk["medical_records"], ["patient_id", "doctor_id", "diagnosis", "prescription", "notes", "visit_date"])
        inserted["prescriptions"] += insert_df(conn, "prescriptions", chunk["prescriptions"], ["patient_id", "doctor_id", "medication_name", "dosage", "frequency", "duration_days", "notes"])
        inserted["lab_tests"] += insert_df(conn, "lab_tests", chunk["lab_tests"], ["patient_id", "doctor_id", "test_name", "status", "result_text", "completed_at"])
        inserted["invoices"] += insert_df(conn, "invoices", chunk["invoices"], ["patient_id", "consultation_fee", "lab_fee", "pharmacy_fee", "total_amount", "status"])
        inserted["reminders"] += insert_df(conn, "reminders", chunk["reminders"], ["patient_id", "channel", "message", "status", "scheduled_for"])
        inserted["triage_queue"] += insert_df(conn, "triage_queue", chunk["triage_queue"], ["patient_id", "priority", "symptoms", "status", "assigned_nurse_id"])
        inserted["nursing_notes"] += insert_df(conn, "nursing_notes", chunk["nursing_notes"], ["patient_id", "nurse_id", "blood_pressure", "temperature", "pulse", "respiratory_rate", "oxygen_saturation", "note"])
        inserted["nurse_tasks"] += insert_df(conn, "nurse_tasks", chunk["nurse_tasks"], ["patient_id", "assigned_nurse_id", "task_title", "task_details", "due_at", "status"])
        conn.commit()

    # Optional standalone extra streams.
    def generate_standalone(table, total, make_rows_fn, columns):
        if total <= 0:
            return 0
        inserted_local = 0
        for off, size in chunk_sizes(total, args.batch_size):
            _ = off
            df = pd.DataFrame(make_rows_fn(size))
            conn.execute("BEGIN")
            inserted_local += insert_df(conn, table, df, columns)
            conn.commit()
        return inserted_local

    def extra_triage_rows(size):
        rows = []
        levels = ["Low", "Medium", "High", "Critical"]
        for _ in range(size):
            rows.append(
                {
                    "patient_id": rng.randint(patient_min, patient_max),
                    "priority": rng.choices(levels, weights=[0.4, 0.34, 0.2, 0.06], k=1)[0],
                    "symptoms": rng.choice(["Fever", "Injury", "Dizziness", "Breathlessness", "Abdominal pain"]),
                    "status": rng.choices(["Waiting", "In Progress", "Completed"], weights=[0.5, 0.25, 0.25], k=1)[0],
                    "assigned_nurse_id": rng.choice(nurse_user_ids) if nurse_user_ids else None,
                }
            )
        return rows

    def extra_note_rows(size):
        rows = []
        for _ in range(size):
            rows.append(
                {
                    "patient_id": rng.randint(patient_min, patient_max),
                    "nurse_id": rng.choice(nurse_user_ids) if nurse_user_ids else 1,
                    "blood_pressure": f"{rng.randint(95, 150)}/{rng.randint(60, 95)}",
                    "temperature": round(rng.uniform(36.0, 39.1), 1),
                    "pulse": rng.randint(55, 120),
                    "respiratory_rate": rng.randint(12, 24),
                    "oxygen_saturation": rng.randint(88, 100),
                    "note": rng.choice(["Patient stable", "Monitor closely", "Mild distress", "Responding to treatment"]),
                }
            )
        return rows

    def extra_task_rows(size):
        rows = []
        for _ in range(size):
            due = datetime.now() + timedelta(hours=rng.randint(1, 72))
            rows.append(
                {
                    "patient_id": rng.randint(patient_min, patient_max),
                    "assigned_nurse_id": rng.choice(nurse_user_ids) if nurse_user_ids else 1,
                    "task_title": rng.choice(["Recheck vitals", "Medication administration", "Wound dressing", "Prep patient", "Monitor intake"]),
                    "task_details": "Generated by micro-ai workload planner",
                    "due_at": due.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": rng.choices(["Pending", "In Progress", "Done"], weights=[0.58, 0.24, 0.18], k=1)[0],
                }
            )
        return rows

    inserted["triage_queue"] += generate_standalone(
        "triage_queue",
        args.triage,
        extra_triage_rows,
        ["patient_id", "priority", "symptoms", "status", "assigned_nurse_id"],
    )
    inserted["nursing_notes"] += generate_standalone(
        "nursing_notes",
        args.nursing_notes,
        extra_note_rows,
        ["patient_id", "nurse_id", "blood_pressure", "temperature", "pulse", "respiratory_rate", "oxygen_saturation", "note"],
    )
    inserted["nurse_tasks"] += generate_standalone(
        "nurse_tasks",
        args.nurse_tasks,
        extra_task_rows,
        ["patient_id", "assigned_nurse_id", "task_title", "task_details", "due_at", "status"],
    )

    totals = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in [
            "users", "patients", "doctors", "appointments", "medical_records", "prescriptions",
            "lab_tests", "invoices", "reminders", "triage_queue", "nursing_notes", "nurse_tasks"
        ]
    }
    conn.close()

    print("Smart seed complete (chunked mode).")
    print(f"Batch size: {args.batch_size:,}")
    print("Inserted this run:")
    print(f"  users: {args.users:,}")
    print(f"  patients: {args.patients:,}")
    print(f"  doctors: {args.doctors:,}")
    for key, val in inserted.items():
        print(f"  {key}: {val:,}")
    print("Table totals:")
    for key, val in totals.items():
        print(f"  {key}: {val:,}")


if __name__ == "__main__":
    main()
