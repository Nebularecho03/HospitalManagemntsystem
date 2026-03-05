import argparse
import hashlib
import random
import sqlite3
from datetime import date, timedelta

try:
    import pandas as pd
except ImportError as exc:
    raise SystemExit(
        "pandas is required. Install it with: ./.venv/bin/pip install pandas"
    ) from exc


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def next_id(conn: sqlite3.Connection, table: str) -> int:
    value = conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()[0]
    return int(value)


def id_range(start: int, count: int):
    return list(range(start, start + count))


def make_users(count: int, start_id: int, prefix: str, rng: random.Random) -> pd.DataFrame:
    roles = ["Patient", "Receptionist", "Doctor", "Patient", "Patient", "Patient"]
    rows = []
    for i, user_id in enumerate(id_range(start_id, count), start=1):
        n = f"{prefix}{user_id}"
        role = roles[i % len(roles)]
        rows.append(
            {
                "username": f"{prefix}_user_{user_id}",
                "password_hash": hash_password("pass123"),
                "role": role,
                "full_name": f"{role} {n}",
                "email": f"{prefix}.user{user_id}@demo.local",
            }
        )
    return pd.DataFrame(rows)


def make_patients(count: int, start_id: int, prefix: str, rng: random.Random) -> pd.DataFrame:
    genders = ["Male", "Female", "Other"]
    rows = []
    for patient_id in id_range(start_id, count):
        rows.append(
            {
                "first_name": f"Pt{prefix}{patient_id}",
                "last_name": f"Demo{patient_id}",
                "age": rng.randint(1, 95),
                "gender": genders[patient_id % len(genders)],
                "phone": f"+1555{patient_id:07d}",
                "email": f"{prefix}.patient{patient_id}@demo.local",
                "address": f"{patient_id} {prefix.title()} Avenue",
            }
        )
    return pd.DataFrame(rows)


def make_doctors(count: int, start_id: int, prefix: str) -> pd.DataFrame:
    specs = [
        "Cardiology",
        "Neurology",
        "Pediatrics",
        "Orthopedics",
        "Dermatology",
        "Oncology",
        "General Medicine",
    ]
    rows = []
    for doctor_id in id_range(start_id, count):
        rows.append(
            {
                "first_name": f"Dr{prefix}{doctor_id}",
                "last_name": f"Medic{doctor_id}",
                "specialization": specs[doctor_id % len(specs)],
                "phone": f"+1666{doctor_id:07d}",
                "email": f"{prefix}.doctor{doctor_id}@demo.local",
                "available_days": "Mon-Fri 09:00-17:00",
            }
        )
    return pd.DataFrame(rows)


def make_appointments(count: int, patient_ids, doctor_ids, rng: random.Random) -> pd.DataFrame:
    statuses = ["Scheduled", "Completed", "Cancelled"]
    base = date.today()
    rows = []
    for i in range(count):
        slot_day = base + timedelta(days=rng.randint(-30, 30))
        hour = rng.choice([8, 9, 10, 11, 13, 14, 15, 16])
        minute = rng.choice(["00", "30"])
        rows.append(
            {
                "patient_id": int(rng.choice(patient_ids)),
                "doctor_id": int(rng.choice(doctor_ids)),
                "appointment_date": slot_day.isoformat(),
                "appointment_time": f"{hour:02d}:{minute}:00",
                "reason": f"Follow-up case #{i+1}",
                "status": statuses[i % len(statuses)],
            }
        )
    return pd.DataFrame(rows)


def make_medical_records(count: int, patient_ids, doctor_ids, rng: random.Random) -> pd.DataFrame:
    base = date.today()
    rows = []
    for i in range(count):
        rows.append(
            {
                "patient_id": int(rng.choice(patient_ids)),
                "doctor_id": int(rng.choice(doctor_ids)),
                "diagnosis": f"Diagnosis {i+1}",
                "prescription": f"Prescription note {i+1}",
                "notes": f"Clinical notes for encounter {i+1}",
                "visit_date": (base - timedelta(days=rng.randint(0, 120))).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def make_prescriptions(count: int, patient_ids, doctor_ids, rng: random.Random) -> pd.DataFrame:
    meds = ["Amoxicillin", "Ibuprofen", "Metformin", "Atorvastatin", "Paracetamol", "Omeprazole"]
    rows = []
    for i in range(count):
        med = meds[i % len(meds)]
        rows.append(
            {
                "patient_id": int(rng.choice(patient_ids)),
                "doctor_id": int(rng.choice(doctor_ids)),
                "medication_name": med,
                "dosage": "500mg",
                "frequency": "Twice daily",
                "duration_days": rng.randint(3, 30),
                "notes": f"Take after meals ({i+1})",
            }
        )
    return pd.DataFrame(rows)


def make_lab_tests(count: int, patient_ids, doctor_ids, rng: random.Random) -> pd.DataFrame:
    tests = ["CBC", "Lipid Panel", "LFT", "KFT", "Thyroid", "HbA1c"]
    rows = []
    for i in range(count):
        rows.append(
            {
                "patient_id": int(rng.choice(patient_ids)),
                "doctor_id": int(rng.choice(doctor_ids)),
                "test_name": tests[i % len(tests)],
                "status": "Ordered",
            }
        )
    return pd.DataFrame(rows)


def make_invoices(count: int, patient_ids, rng: random.Random) -> pd.DataFrame:
    rows = []
    for _ in range(count):
        consult = round(rng.uniform(30, 120), 2)
        lab = round(rng.uniform(0, 200), 2)
        pharm = round(rng.uniform(0, 150), 2)
        total = round(consult + lab + pharm, 2)
        rows.append(
            {
                "patient_id": int(rng.choice(patient_ids)),
                "consultation_fee": consult,
                "lab_fee": lab,
                "pharmacy_fee": pharm,
                "total_amount": total,
                "status": rng.choice(["Unpaid", "Paid"]),
            }
        )
    return pd.DataFrame(rows)


def make_attachments(count: int, patient_ids, user_ids, prefix: str, rng: random.Random) -> pd.DataFrame:
    rows = []
    for i in range(count):
        rows.append(
            {
                "patient_id": int(rng.choice(patient_ids)),
                "title": f"Report {i+1}",
                "file_url": f"https://files.demo.local/{prefix}/report-{i+1}.pdf",
                "uploaded_by": int(rng.choice(user_ids)),
            }
        )
    return pd.DataFrame(rows)


def make_reminders(count: int, patient_ids, appointment_ids, rng: random.Random) -> pd.DataFrame:
    channels = ["SMS", "Email"]
    rows = []
    for i in range(count):
        rows.append(
            {
                "patient_id": int(rng.choice(patient_ids)),
                "appointment_id": int(rng.choice(appointment_ids)) if appointment_ids else None,
                "channel": channels[i % len(channels)],
                "message": f"Reminder message #{i+1}",
                "status": "Queued",
                "scheduled_for": None,
            }
        )
    return pd.DataFrame(rows)


def insert_df(conn: sqlite3.Connection, df: pd.DataFrame, table: str, columns):
    placeholders = ", ".join(["?"] * len(columns))
    col_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"
    conn.executemany(sql, df[columns].itertuples(index=False, name=None))


def main():
    parser = argparse.ArgumentParser(description="Seed demo HMS data using pandas.")
    parser.add_argument("--db-path", default="hospital.db", help="SQLite DB path")
    parser.add_argument("--count", type=int, default=500, help="Number of rows per major entity")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--prefix", default="bulk", help="Prefix to make generated usernames/emails unique")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    user_start = next_id(conn, "users")
    patient_start = next_id(conn, "patients")
    doctor_start = next_id(conn, "doctors")

    users_df = make_users(args.count, user_start, args.prefix, rng)
    patients_df = make_patients(args.count, patient_start, args.prefix, rng)
    doctors_df = make_doctors(args.count, doctor_start, args.prefix)

    conn.execute("BEGIN")
    insert_df(conn, users_df, "users", ["username", "password_hash", "role", "full_name", "email"])
    insert_df(conn, patients_df, "patients", ["first_name", "last_name", "age", "gender", "phone", "email", "address"])
    insert_df(conn, doctors_df, "doctors", ["first_name", "last_name", "specialization", "phone", "email", "available_days"])
    conn.commit()

    patient_ids = id_range(patient_start, args.count)
    doctor_ids = id_range(doctor_start, args.count)
    user_ids = id_range(user_start, args.count)

    appointments_df = make_appointments(args.count, patient_ids, doctor_ids, rng)
    records_df = make_medical_records(args.count, patient_ids, doctor_ids, rng)
    prescriptions_df = make_prescriptions(args.count, patient_ids, doctor_ids, rng)
    lab_tests_df = make_lab_tests(args.count, patient_ids, doctor_ids, rng)
    invoices_df = make_invoices(args.count, patient_ids, rng)

    conn.execute("BEGIN")
    insert_df(
        conn,
        appointments_df,
        "appointments",
        ["patient_id", "doctor_id", "appointment_date", "appointment_time", "reason", "status"],
    )
    insert_df(
        conn,
        records_df,
        "medical_records",
        ["patient_id", "doctor_id", "diagnosis", "prescription", "notes", "visit_date"],
    )
    insert_df(
        conn,
        prescriptions_df,
        "prescriptions",
        ["patient_id", "doctor_id", "medication_name", "dosage", "frequency", "duration_days", "notes"],
    )
    insert_df(
        conn,
        lab_tests_df,
        "lab_tests",
        ["patient_id", "doctor_id", "test_name", "status"],
    )
    insert_df(
        conn,
        invoices_df,
        "invoices",
        ["patient_id", "consultation_fee", "lab_fee", "pharmacy_fee", "total_amount", "status"],
    )
    conn.commit()

    appointment_start = next_id(conn, "appointments") - args.count
    appointment_ids = id_range(appointment_start, args.count)

    attachments_df = make_attachments(args.count, patient_ids, user_ids, args.prefix, rng)
    reminders_df = make_reminders(args.count, patient_ids, appointment_ids, rng)

    conn.execute("BEGIN")
    insert_df(conn, attachments_df, "attachments", ["patient_id", "title", "file_url", "uploaded_by"])
    insert_df(conn, reminders_df, "reminders", ["patient_id", "appointment_id", "channel", "message", "status", "scheduled_for"])
    conn.commit()

    counts = {
        "users": conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "patients": conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0],
        "doctors": conn.execute("SELECT COUNT(*) FROM doctors").fetchone()[0],
        "appointments": conn.execute("SELECT COUNT(*) FROM appointments").fetchone()[0],
        "medical_records": conn.execute("SELECT COUNT(*) FROM medical_records").fetchone()[0],
        "prescriptions": conn.execute("SELECT COUNT(*) FROM prescriptions").fetchone()[0],
        "lab_tests": conn.execute("SELECT COUNT(*) FROM lab_tests").fetchone()[0],
        "invoices": conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0],
        "attachments": conn.execute("SELECT COUNT(*) FROM attachments").fetchone()[0],
        "reminders": conn.execute("SELECT COUNT(*) FROM reminders").fetchone()[0],
    }
    conn.close()

    print("Seed completed successfully.")
    print(f"Inserted per entity: {args.count}")
    print("Current table totals:")
    for k, v in counts.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
