import argparse
import os
import sqlite3
import sys
import time
from datetime import date, timedelta

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.database import Database


def timer():
    return time.perf_counter()


def insert_doctors(conn, count, start_index, batch_size):
    sql = """
        INSERT INTO doctors (first_name, last_name, specialization, phone, email, available_days)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    specializations = [
        "Cardiology",
        "Neurology",
        "Orthopedics",
        "Pediatrics",
        "General Medicine",
        "Dermatology",
        "Oncology",
        "Radiology",
    ]

    inserted = 0
    current = start_index
    while inserted < count:
        size = min(batch_size, count - inserted)
        rows = []
        for _ in range(size):
            idx = current
            rows.append(
                (
                    f"Doctor{idx}",
                    f"Stress{idx}",
                    specializations[idx % len(specializations)],
                    f"+1555{idx:07d}",
                    f"doctor{idx}@stress.test",
                    "Mon-Fri 9AM-5PM",
                )
            )
            current += 1

        conn.executemany(sql, rows)
        inserted += size

    return inserted


def insert_patients(conn, count, start_index, batch_size):
    sql = """
        INSERT INTO patients (first_name, last_name, age, gender, phone, email, address)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    genders = ["Male", "Female", "Other"]

    inserted = 0
    current = start_index
    while inserted < count:
        size = min(batch_size, count - inserted)
        rows = []
        for _ in range(size):
            idx = current
            rows.append(
                (
                    f"Patient{idx}",
                    f"Load{idx}",
                    18 + (idx % 70),
                    genders[idx % len(genders)],
                    f"+1444{idx:07d}",
                    f"patient{idx}@stress.test",
                    f"{idx} Load Testing Street",
                )
            )
            current += 1

        conn.executemany(sql, rows)
        inserted += size

    return inserted


def insert_appointments(conn, count, patient_min, patient_max, doctor_min, doctor_max, batch_size):
    sql = """
        INSERT INTO appointments (patient_id, doctor_id, appointment_date, appointment_time, reason, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    statuses = ["Scheduled", "Completed", "Cancelled"]
    today = date.today()

    patient_span = (patient_max - patient_min) + 1
    doctor_span = (doctor_max - doctor_min) + 1

    inserted = 0
    i = 0
    while inserted < count:
        size = min(batch_size, count - inserted)
        rows = []
        for _ in range(size):
            patient_id = patient_min + (i % patient_span)
            doctor_id = doctor_min + ((i * 7) % doctor_span)
            appointment_day = today + timedelta(days=(i % 120) - 60)
            hour = 8 + (i % 10)
            minute = 30 if i % 2 == 0 else 0
            rows.append(
                (
                    patient_id,
                    doctor_id,
                    appointment_day.isoformat(),
                    f"{hour:02d}:{minute:02d}:00",
                    f"Reason #{i}",
                    statuses[i % len(statuses)],
                )
            )
            i += 1

        conn.executemany(sql, rows)
        inserted += size

    return inserted


def insert_medical_records(conn, count, patient_min, patient_max, doctor_min, doctor_max, batch_size):
    sql = """
        INSERT INTO medical_records (patient_id, doctor_id, diagnosis, prescription, notes, visit_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    today = date.today()

    patient_span = (patient_max - patient_min) + 1
    doctor_span = (doctor_max - doctor_min) + 1

    inserted = 0
    i = 0
    while inserted < count:
        size = min(batch_size, count - inserted)
        rows = []
        for _ in range(size):
            patient_id = patient_min + (i % patient_span)
            doctor_id = doctor_min + ((i * 11) % doctor_span)
            visit_day = today - timedelta(days=i % 365)
            rows.append(
                (
                    patient_id,
                    doctor_id,
                    f"Diagnosis #{i % 500}",
                    f"Prescription #{i % 1000}",
                    f"Stress test note block {i}",
                    visit_day.isoformat(),
                )
            )
            i += 1

        conn.executemany(sql, rows)
        inserted += size

    return inserted


def scalar(conn, query):
    return conn.execute(query).fetchone()[0]


def get_id_range(conn, table):
    row = conn.execute(f"SELECT MIN(id), MAX(id) FROM {table}").fetchone()
    return row[0], row[1]


def benchmark_query(conn, query, label):
    start = timer()
    rows = conn.execute(query).fetchall()
    elapsed = timer() - start
    return {"label": label, "rows": len(rows), "seconds": round(elapsed, 4)}


def main():
    parser = argparse.ArgumentParser(description="Stress test SQLite hospital database.")
    parser.add_argument("--db-path", default="load_test_hospital.db", help="Path to SQLite database file")
    parser.add_argument("--patients", type=int, default=100000)
    parser.add_argument("--doctors", type=int, default=5000)
    parser.add_argument("--appointments", type=int, default=300000)
    parser.add_argument("--records", type=int, default=200000)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--reset", action="store_true", help="Delete existing db file before running")
    args = parser.parse_args()

    if args.reset and os.path.exists(args.db_path):
        os.remove(args.db_path)

    Database(db_name=args.db_path)
    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    doctor_start = scalar(conn, "SELECT COALESCE(MAX(id), 0) + 1 FROM doctors")
    patient_start = scalar(conn, "SELECT COALESCE(MAX(id), 0) + 1 FROM patients")

    t0 = timer()
    conn.execute("BEGIN")
    inserted_doctors = insert_doctors(conn, args.doctors, doctor_start, args.batch_size)
    conn.commit()
    doctors_time = timer() - t0

    t0 = timer()
    conn.execute("BEGIN")
    inserted_patients = insert_patients(conn, args.patients, patient_start, args.batch_size)
    conn.commit()
    patients_time = timer() - t0

    if inserted_doctors > 0:
        doctor_min = doctor_start
        doctor_max = doctor_start + inserted_doctors - 1
    else:
        doctor_min, doctor_max = get_id_range(conn, "doctors")

    if inserted_patients > 0:
        patient_min = patient_start
        patient_max = patient_start + inserted_patients - 1
    else:
        patient_min, patient_max = get_id_range(conn, "patients")

    if args.appointments > 0 or args.records > 0:
        if doctor_min is None or doctor_max is None or patient_min is None or patient_max is None:
            raise RuntimeError(
                "Cannot create appointments/records without at least one patient and one doctor in the database."
            )

    t0 = timer()
    conn.execute("BEGIN")
    inserted_appointments = insert_appointments(
        conn,
        args.appointments,
        patient_min,
        patient_max,
        doctor_min,
        doctor_max,
        args.batch_size,
    )
    conn.commit()
    appointments_time = timer() - t0

    t0 = timer()
    conn.execute("BEGIN")
    inserted_records = insert_medical_records(
        conn,
        args.records,
        patient_min,
        patient_max,
        doctor_min,
        doctor_max,
        args.batch_size,
    )
    conn.commit()
    records_time = timer() - t0

    bench = [
        benchmark_query(conn, "SELECT COUNT(*) FROM patients", "Count patients"),
        benchmark_query(conn, "SELECT COUNT(*) FROM doctors", "Count doctors"),
        benchmark_query(conn, "SELECT COUNT(*) FROM appointments WHERE status='Scheduled'", "Count scheduled appointments"),
        benchmark_query(
            conn,
            """
            SELECT a.id, p.first_name, p.last_name, d.first_name, d.last_name
            FROM appointments a
            JOIN patients p ON a.patient_id = p.id
            JOIN doctors d ON a.doctor_id = d.id
            ORDER BY a.appointment_date DESC, a.appointment_time DESC
            LIMIT 1000
            """,
            "Join latest appointments (1000)",
        ),
    ]

    total_patients = scalar(conn, "SELECT COUNT(*) FROM patients")
    total_doctors = scalar(conn, "SELECT COUNT(*) FROM doctors")
    total_appointments = scalar(conn, "SELECT COUNT(*) FROM appointments")
    total_records = scalar(conn, "SELECT COUNT(*) FROM medical_records")

    conn.close()

    db_size_mb = os.path.getsize(args.db_path) / (1024 * 1024)
    total_inserted = inserted_doctors + inserted_patients + inserted_appointments + inserted_records

    print("=== SQLite Stress Test Summary ===")
    print(f"Database file: {os.path.abspath(args.db_path)}")
    print(f"Database size: {db_size_mb:.2f} MB")
    print("")
    print("Inserted this run:")
    print(f"  Doctors:      {inserted_doctors:,} in {doctors_time:.2f}s ({inserted_doctors / max(doctors_time, 1e-9):,.0f} rows/s)")
    print(f"  Patients:     {inserted_patients:,} in {patients_time:.2f}s ({inserted_patients / max(patients_time, 1e-9):,.0f} rows/s)")
    print(f"  Appointments: {inserted_appointments:,} in {appointments_time:.2f}s ({inserted_appointments / max(appointments_time, 1e-9):,.0f} rows/s)")
    print(f"  Records:      {inserted_records:,} in {records_time:.2f}s ({inserted_records / max(records_time, 1e-9):,.0f} rows/s)")
    print(f"  Total:        {total_inserted:,}")
    print("")
    print("Total table sizes:")
    print(f"  patients:        {total_patients:,}")
    print(f"  doctors:         {total_doctors:,}")
    print(f"  appointments:    {total_appointments:,}")
    print(f"  medical_records: {total_records:,}")
    print("")
    print("Query benchmarks:")
    for item in bench:
        print(f"  {item['label']}: {item['seconds']:.4f}s ({item['rows']} row set)")


if __name__ == "__main__":
    main()
