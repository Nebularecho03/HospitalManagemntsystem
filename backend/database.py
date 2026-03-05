import hashlib
import secrets
import sqlite3
from datetime import datetime, timedelta


class Database:
    def __init__(self, db_name="hospital.db"):
        self.db_name = db_name
        self.init_database()
        self.ensure_default_admin()
        self.ensure_default_portal_users()

    def get_connection(self):
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @staticmethod
    def _hash_password(password):
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('Admin', 'Doctor', 'Receptionist', 'Patient', 'Nurse')),
                full_name TEXT,
                email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._ensure_users_role_supports_nurse(conn)
        self._repair_user_foreign_keys(conn)

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token TEXT UNIQUE NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS patients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                age INTEGER,
                gender TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS doctors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                specialization TEXT,
                phone TEXT,
                email TEXT,
                available_days TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS doctor_slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doctor_id INTEGER NOT NULL,
                slot_date DATE NOT NULL,
                slot_time TIME NOT NULL,
                is_available INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(doctor_id, slot_date, slot_time),
                FOREIGN KEY (doctor_id) REFERENCES doctors (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                doctor_id INTEGER NOT NULL,
                appointment_date DATE NOT NULL,
                appointment_time TIME NOT NULL,
                reason TEXT,
                status TEXT DEFAULT 'Scheduled',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (doctor_id) REFERENCES doctors (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS medical_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                doctor_id INTEGER NOT NULL,
                diagnosis TEXT,
                prescription TEXT,
                notes TEXT,
                visit_date DATE DEFAULT CURRENT_DATE,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (doctor_id) REFERENCES doctors (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prescriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                doctor_id INTEGER NOT NULL,
                medication_name TEXT NOT NULL,
                dosage TEXT,
                frequency TEXT,
                duration_days INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (doctor_id) REFERENCES doctors (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS lab_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                doctor_id INTEGER NOT NULL,
                test_name TEXT NOT NULL,
                status TEXT DEFAULT 'Ordered',
                result_text TEXT,
                result_file_url TEXT,
                ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (doctor_id) REFERENCES doctors (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                file_url TEXT NOT NULL,
                uploaded_by INTEGER,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (uploaded_by) REFERENCES users (id) ON DELETE SET NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                appointment_id INTEGER,
                channel TEXT NOT NULL CHECK(channel IN ('SMS', 'Email')),
                message TEXT NOT NULL,
                status TEXT DEFAULT 'Queued',
                scheduled_for TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (appointment_id) REFERENCES appointments (id) ON DELETE SET NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                consultation_fee REAL DEFAULT 0,
                lab_fee REAL DEFAULT 0,
                pharmacy_fee REAL DEFAULT 0,
                total_amount REAL NOT NULL,
                status TEXT DEFAULT 'Unpaid',
                issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                paid_at TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                entity_type TEXT,
                entity_id INTEGER,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE SET NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS triage_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                priority TEXT NOT NULL CHECK(priority IN ('Low', 'Medium', 'High', 'Critical')),
                symptoms TEXT,
                status TEXT DEFAULT 'Waiting' CHECK(status IN ('Waiting', 'In Progress', 'Completed')),
                assigned_nurse_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_nurse_id) REFERENCES users (id) ON DELETE SET NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS nursing_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER NOT NULL,
                nurse_id INTEGER NOT NULL,
                blood_pressure TEXT,
                temperature REAL,
                pulse INTEGER,
                respiratory_rate INTEGER,
                oxygen_saturation INTEGER,
                note TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE CASCADE,
                FOREIGN KEY (nurse_id) REFERENCES users (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS nurse_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id INTEGER,
                assigned_nurse_id INTEGER NOT NULL,
                task_title TEXT NOT NULL,
                task_details TEXT,
                due_at TIMESTAMP,
                status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending', 'In Progress', 'Done')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (patient_id) REFERENCES patients (id) ON DELETE SET NULL,
                FOREIGN KEY (assigned_nurse_id) REFERENCES users (id) ON DELETE CASCADE
            )
            """
        )

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_patients_name ON patients(last_name, first_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_appointments_date_time ON appointments(appointment_date, appointment_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_appointments_doctor_slot ON appointments(doctor_id, appointment_date, appointment_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_medical_records_patient ON medical_records(patient_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_lab_tests_patient ON lab_tests(patient_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_invoices_patient ON invoices(patient_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_triage_status_priority ON triage_queue(status, priority)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nursing_notes_patient ON nursing_notes(patient_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nurse_tasks_nurse ON nurse_tasks(assigned_nurse_id, status)")

        conn.commit()
        conn.close()

    def _ensure_users_role_supports_nurse(self, conn):
        cursor = conn.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='users'")
        row = cursor.fetchone()
        if not row:
            return

        sql = row[0] or ""
        if "'Nurse'" in sql:
            return

        cursor.execute("PRAGMA foreign_keys = OFF")
        cursor.execute("ALTER TABLE users RENAME TO users_old")
        cursor.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('Admin', 'Doctor', 'Receptionist', 'Patient', 'Nurse')),
                full_name TEXT,
                email TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO users (id, username, password_hash, role, full_name, email, created_at)
            SELECT id, username, password_hash, role, full_name, email, created_at
            FROM users_old
            """
        )
        cursor.execute("DROP TABLE users_old")
        cursor.execute("PRAGMA foreign_keys = ON")

    def _repair_user_foreign_keys(self, conn):
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND sql LIKE '%users_old%'"
        )
        broken_tables = cursor.fetchall()
        if not broken_tables:
            return

        cursor.execute("PRAGMA foreign_keys = OFF")
        for table_name, create_sql in broken_tables:
            temp_name = f"{table_name}__fk_fix"
            cursor.execute(f"ALTER TABLE {table_name} RENAME TO {temp_name}")

            fixed_sql = create_sql.replace('"users_old"', "users").replace("users_old", "users")
            cursor.execute(fixed_sql)

            columns = [row[1] for row in cursor.execute(f"PRAGMA table_info({temp_name})").fetchall()]
            col_csv = ", ".join(columns)
            cursor.execute(f"INSERT INTO {table_name} ({col_csv}) SELECT {col_csv} FROM {temp_name}")
            cursor.execute(f"DROP TABLE {temp_name}")

        cursor.execute("PRAGMA foreign_keys = ON")

    def ensure_default_admin(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        has_users = cursor.fetchone()[0] > 0
        if not has_users:
            cursor.execute(
                """
                INSERT INTO users (username, password_hash, role, full_name, email)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("admin", self._hash_password("admin123"), "Admin", "System Admin", "admin@hospital.local"),
            )
            conn.commit()
        conn.close()

    def ensure_default_portal_users(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        default_users = [
            ("doctor1", "doctor123", "Doctor", "Dr. Portal", "doctor1@hospital.local"),
            ("nurse1", "nurse123", "Nurse", "Nurse Portal", "nurse1@hospital.local"),
            ("reception1", "reception123", "Receptionist", "Front Desk", "reception1@hospital.local"),
        ]
        for username, password, role, full_name, email in default_users:
            cursor.execute(
                """
                INSERT OR IGNORE INTO users (username, password_hash, role, full_name, email)
                VALUES (?, ?, ?, ?, ?)
                """,
                (username, self._hash_password(password), role, full_name, email),
            )
        conn.commit()
        conn.close()

    def create_user(self, username, password, role, full_name=None, email=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO users (username, password_hash, role, full_name, email)
            VALUES (?, ?, ?, ?, ?)
            """,
            (username, self._hash_password(password), role, full_name, email),
        )
        conn.commit()
        user_id = cursor.lastrowid
        conn.close()
        return user_id

    def authenticate_user(self, username, password):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, username, role, full_name, email FROM users WHERE username = ? AND password_hash = ?",
            (username, self._hash_password(password)),
        )
        user = cursor.fetchone()
        conn.close()
        return user

    def create_token(self, user_id, hours=12):
        token = secrets.token_hex(24)
        expires_at = (datetime.utcnow() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")

        conn = self.get_connection()
        conn.execute(
            "INSERT INTO auth_tokens (user_id, token, expires_at) VALUES (?, ?, ?)",
            (user_id, token, expires_at),
        )
        conn.commit()
        conn.close()
        return token, expires_at

    def get_user_by_token(self, token):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT u.id, u.username, u.role, u.full_name, u.email
            FROM auth_tokens t
            JOIN users u ON u.id = t.user_id
            WHERE t.token = ? AND datetime(t.expires_at) > datetime('now')
            """,
            (token,),
        )
        row = cursor.fetchone()
        conn.close()
        return row

    def log_audit(self, user_id, action, entity_type=None, entity_id=None, details=None):
        conn = self.get_connection()
        conn.execute(
            """
            INSERT INTO audit_logs (user_id, action, entity_type, entity_id, details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, action, entity_type, entity_id, details),
        )
        conn.commit()
        conn.close()

    def get_audit_logs(self, limit=200):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT al.*, u.username, u.role
            FROM audit_logs al
            LEFT JOIN users u ON u.id = al.user_id
            ORDER BY al.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        conn.close()
        return rows

    def add_patient(self, data):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO patients (first_name, last_name, age, gender, phone, email, address)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["first_name"],
                data["last_name"],
                data.get("age"),
                data.get("gender"),
                data.get("phone"),
                data.get("email"),
                data.get("address"),
            ),
        )
        conn.commit()
        patient_id = cursor.lastrowid
        conn.close()
        return patient_id

    def get_all_patients(self, filters=None):
        filters = filters or {}
        conn = self.get_connection()
        cursor = conn.cursor()

        sql = """
            SELECT p.*, (
                SELECT MAX(a.appointment_date)
                FROM appointments a
                WHERE a.patient_id = p.id
            ) AS last_visit
            FROM patients p
            WHERE 1=1
        """
        params = []

        if filters.get("name"):
            sql += " AND (p.first_name LIKE ? OR p.last_name LIKE ?)"
            token = f"%{filters['name']}%"
            params.extend([token, token])

        if filters.get("phone"):
            sql += " AND p.phone LIKE ?"
            params.append(f"%{filters['phone']}%")

        if filters.get("min_age") is not None:
            sql += " AND p.age >= ?"
            params.append(filters["min_age"])

        if filters.get("max_age") is not None:
            sql += " AND p.age <= ?"
            params.append(filters["max_age"])

        if filters.get("last_visit_days") is not None:
            sql += """
                AND EXISTS (
                    SELECT 1
                    FROM appointments a
                    WHERE a.patient_id = p.id
                      AND date(a.appointment_date) >= date('now', ?)
                )
            """
            params.append(f"-{int(filters['last_visit_days'])} days")

        sql += " ORDER BY p.last_name, p.first_name"

        cursor.execute(sql, params)
        rows = cursor.fetchall()
        conn.close()
        return rows

    def get_patient(self, patient_id):
        conn = self.get_connection()
        row = conn.execute("SELECT * FROM patients WHERE id = ?", (patient_id,)).fetchone()
        conn.close()
        return row

    def update_patient(self, patient_id, data):
        conn = self.get_connection()
        conn.execute(
            """
            UPDATE patients
            SET first_name=?, last_name=?, age=?, gender=?, phone=?, email=?, address=?
            WHERE id=?
            """,
            (
                data["first_name"],
                data["last_name"],
                data.get("age"),
                data.get("gender"),
                data.get("phone"),
                data.get("email"),
                data.get("address"),
                patient_id,
            ),
        )
        conn.commit()
        conn.close()

    def delete_patient(self, patient_id):
        conn = self.get_connection()
        conn.execute("DELETE FROM patients WHERE id = ?", (patient_id,))
        conn.commit()
        conn.close()

    def add_doctor(self, data):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO doctors (first_name, last_name, specialization, phone, email, available_days)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                data["first_name"],
                data["last_name"],
                data.get("specialization"),
                data.get("phone"),
                data.get("email"),
                data.get("available_days"),
            ),
        )
        conn.commit()
        doctor_id = cursor.lastrowid
        conn.close()
        return doctor_id

    def get_all_doctors(self):
        conn = self.get_connection()
        rows = conn.execute("SELECT * FROM doctors ORDER BY last_name, first_name").fetchall()
        conn.close()
        return rows

    def get_doctor(self, doctor_id):
        conn = self.get_connection()
        row = conn.execute("SELECT * FROM doctors WHERE id = ?", (doctor_id,)).fetchone()
        conn.close()
        return row

    def add_doctor_slot(self, doctor_id, slot_date, slot_time):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO doctor_slots (doctor_id, slot_date, slot_time, is_available)
            VALUES (?, ?, ?, 1)
            """,
            (doctor_id, slot_date, slot_time),
        )
        conn.commit()
        slot_id = cursor.lastrowid
        conn.close()
        return slot_id

    def get_doctor_slots(self, doctor_id):
        conn = self.get_connection()
        rows = conn.execute(
            "SELECT * FROM doctor_slots WHERE doctor_id = ? ORDER BY slot_date, slot_time",
            (doctor_id,),
        ).fetchall()
        conn.close()
        return rows

    def add_appointment(self, data):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM appointments
            WHERE doctor_id = ?
              AND appointment_date = ?
              AND appointment_time = ?
              AND status = 'Scheduled'
            """,
            (data["doctor_id"], data["appointment_date"], data["appointment_time"]),
        )
        exists = cursor.fetchone()[0] > 0
        if exists:
            conn.close()
            raise ValueError("Doctor already has a scheduled appointment for this date/time")

        cursor.execute(
            """
            INSERT INTO appointments (patient_id, doctor_id, appointment_date, appointment_time, reason, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                data["patient_id"],
                data["doctor_id"],
                data["appointment_date"],
                data["appointment_time"],
                data.get("reason"),
                data.get("status", "Scheduled"),
            ),
        )
        conn.commit()
        appointment_id = cursor.lastrowid
        conn.close()
        return appointment_id

    def get_all_appointments(self):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT a.*, p.first_name AS patient_first, p.last_name AS patient_last,
                   d.first_name AS doctor_first, d.last_name AS doctor_last
            FROM appointments a
            JOIN patients p ON a.patient_id = p.id
            JOIN doctors d ON a.doctor_id = d.id
            ORDER BY a.appointment_date DESC, a.appointment_time DESC
            """
        ).fetchall()
        conn.close()
        return rows

    def update_appointment_status(self, appointment_id, status):
        conn = self.get_connection()
        conn.execute("UPDATE appointments SET status = ? WHERE id = ?", (status, appointment_id))
        conn.commit()
        conn.close()

    def add_medical_record(self, data):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO medical_records (patient_id, doctor_id, diagnosis, prescription, notes, visit_date)
            VALUES (?, ?, ?, ?, ?, COALESCE(?, date('now')))
            """,
            (
                data["patient_id"],
                data["doctor_id"],
                data.get("diagnosis"),
                data.get("prescription"),
                data.get("notes"),
                data.get("visit_date"),
            ),
        )
        conn.commit()
        record_id = cursor.lastrowid
        conn.close()
        return record_id

    def get_patient_records(self, patient_id):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT mr.*, d.first_name AS doctor_first, d.last_name AS doctor_last
            FROM medical_records mr
            JOIN doctors d ON mr.doctor_id = d.id
            WHERE mr.patient_id = ?
            ORDER BY mr.visit_date DESC
            """,
            (patient_id,),
        ).fetchall()
        conn.close()
        return rows

    def add_prescription(self, data):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO prescriptions (
                patient_id, doctor_id, medication_name, dosage, frequency, duration_days, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["patient_id"],
                data["doctor_id"],
                data["medication_name"],
                data.get("dosage"),
                data.get("frequency"),
                data.get("duration_days"),
                data.get("notes"),
            ),
        )
        conn.commit()
        prescription_id = cursor.lastrowid
        conn.close()
        return prescription_id

    def get_patient_prescriptions(self, patient_id):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT pr.*, d.first_name AS doctor_first, d.last_name AS doctor_last
            FROM prescriptions pr
            JOIN doctors d ON pr.doctor_id = d.id
            WHERE pr.patient_id = ?
            ORDER BY pr.created_at DESC
            """,
            (patient_id,),
        ).fetchall()
        conn.close()
        return rows

    def add_lab_test(self, data):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO lab_tests (patient_id, doctor_id, test_name, status)
            VALUES (?, ?, ?, 'Ordered')
            """,
            (data["patient_id"], data["doctor_id"], data["test_name"]),
        )
        conn.commit()
        test_id = cursor.lastrowid
        conn.close()
        return test_id

    def update_lab_result(self, test_id, result_text=None, result_file_url=None):
        conn = self.get_connection()
        conn.execute(
            """
            UPDATE lab_tests
            SET result_text = ?, result_file_url = ?, status = 'Completed', completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (result_text, result_file_url, test_id),
        )
        conn.commit()
        conn.close()

    def get_patient_lab_tests(self, patient_id):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT lt.*, d.first_name AS doctor_first, d.last_name AS doctor_last
            FROM lab_tests lt
            JOIN doctors d ON lt.doctor_id = d.id
            WHERE lt.patient_id = ?
            ORDER BY lt.ordered_at DESC
            """,
            (patient_id,),
        ).fetchall()
        conn.close()
        return rows

    def add_attachment(self, data, uploaded_by=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO attachments (patient_id, title, file_url, uploaded_by)
            VALUES (?, ?, ?, ?)
            """,
            (data["patient_id"], data["title"], data["file_url"], uploaded_by),
        )
        conn.commit()
        attachment_id = cursor.lastrowid
        conn.close()
        return attachment_id

    def get_patient_attachments(self, patient_id):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT a.*, u.username
            FROM attachments a
            LEFT JOIN users u ON a.uploaded_by = u.id
            WHERE a.patient_id = ?
            ORDER BY a.uploaded_at DESC
            """,
            (patient_id,),
        ).fetchall()
        conn.close()
        return rows

    def create_reminder(self, data):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO reminders (patient_id, appointment_id, channel, message, status, scheduled_for)
            VALUES (?, ?, ?, ?, 'Queued', ?)
            """,
            (
                data["patient_id"],
                data.get("appointment_id"),
                data["channel"],
                data["message"],
                data.get("scheduled_for"),
            ),
        )
        conn.commit()
        reminder_id = cursor.lastrowid
        conn.close()
        return reminder_id

    def get_reminders(self):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT r.*, p.first_name AS patient_first, p.last_name AS patient_last
            FROM reminders r
            JOIN patients p ON r.patient_id = p.id
            ORDER BY r.created_at DESC
            """
        ).fetchall()
        conn.close()
        return rows

    def add_invoice(self, data):
        consultation_fee = float(data.get("consultation_fee", 0) or 0)
        lab_fee = float(data.get("lab_fee", 0) or 0)
        pharmacy_fee = float(data.get("pharmacy_fee", 0) or 0)
        total = consultation_fee + lab_fee + pharmacy_fee

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO invoices (patient_id, consultation_fee, lab_fee, pharmacy_fee, total_amount, status)
            VALUES (?, ?, ?, ?, ?, 'Unpaid')
            """,
            (data["patient_id"], consultation_fee, lab_fee, pharmacy_fee, total),
        )
        conn.commit()
        invoice_id = cursor.lastrowid
        conn.close()
        return invoice_id

    def mark_invoice_paid(self, invoice_id):
        conn = self.get_connection()
        conn.execute(
            """
            UPDATE invoices
            SET status = 'Paid', paid_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (invoice_id,),
        )
        conn.commit()
        conn.close()

    def get_invoices(self):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT i.*, p.first_name AS patient_first, p.last_name AS patient_last
            FROM invoices i
            JOIN patients p ON i.patient_id = p.id
            ORDER BY i.issued_at DESC
            """
        ).fetchall()
        conn.close()
        return rows

    def get_patient_invoices(self, patient_id):
        conn = self.get_connection()
        rows = conn.execute(
            "SELECT * FROM invoices WHERE patient_id = ? ORDER BY issued_at DESC",
            (patient_id,),
        ).fetchall()
        conn.close()
        return rows

    def get_dashboard_stats(self):
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM patients")
        total_patients = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM doctors")
        total_doctors = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM appointments WHERE status = 'Scheduled'")
        pending_appointments = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM appointments WHERE date(appointment_date) = date('now')")
        today_appointments = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM appointments")
        total_appointments = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM appointments WHERE status = 'Cancelled'")
        cancelled_appointments = cursor.fetchone()[0]

        cursor.execute("SELECT COALESCE(SUM(total_amount), 0) FROM invoices WHERE status = 'Paid'")
        revenue = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM triage_queue WHERE status != 'Completed'")
        open_triage_cases = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM nurse_tasks WHERE status != 'Done'")
        open_nurse_tasks = cursor.fetchone()[0]

        no_show_rate = 0
        if total_appointments > 0:
            no_show_rate = round((cancelled_appointments / total_appointments) * 100, 2)

        conn.close()

        return {
            "total_patients": total_patients,
            "total_doctors": total_doctors,
            "pending_appointments": pending_appointments,
            "today_appointments": today_appointments,
            "total_appointments": total_appointments,
            "cancelled_appointments": cancelled_appointments,
            "no_show_rate": no_show_rate,
            "paid_revenue": revenue,
            "open_triage_cases": open_triage_cases,
            "open_nurse_tasks": open_nurse_tasks,
        }

    def get_admin_analytics(self, days=14):
        conn = self.get_connection()
        cursor = conn.cursor()

        # Appointment volumes per day
        daily_appointments = cursor.execute(
            """
            SELECT appointment_date AS day, COUNT(*) AS total
            FROM appointments
            WHERE date(appointment_date) >= date('now', ?)
            GROUP BY appointment_date
            ORDER BY appointment_date
            """,
            (f"-{int(days)} days",),
        ).fetchall()

        # Paid revenue per day
        daily_revenue = cursor.execute(
            """
            SELECT date(issued_at) AS day, COALESCE(SUM(total_amount), 0) AS total
            FROM invoices
            WHERE status = 'Paid'
              AND date(issued_at) >= date('now', ?)
            GROUP BY date(issued_at)
            ORDER BY date(issued_at)
            """,
            (f"-{int(days)} days",),
        ).fetchall()

        appointment_status = cursor.execute(
            """
            SELECT status, COUNT(*) AS total
            FROM appointments
            GROUP BY status
            ORDER BY total DESC
            """
        ).fetchall()

        triage_priority = cursor.execute(
            """
            SELECT priority, COUNT(*) AS total
            FROM triage_queue
            GROUP BY priority
            ORDER BY
                CASE priority
                    WHEN 'Critical' THEN 1
                    WHEN 'High' THEN 2
                    WHEN 'Medium' THEN 3
                    ELSE 4
                END
            """
        ).fetchall()

        top_doctor_workload = cursor.execute(
            """
            SELECT d.first_name || ' ' || d.last_name AS doctor_name, COUNT(*) AS total
            FROM appointments a
            JOIN doctors d ON d.id = a.doctor_id
            WHERE date(a.appointment_date) >= date('now', ?)
            GROUP BY d.id
            ORDER BY total DESC
            LIMIT 8
            """,
            (f"-{int(days)} days",),
        ).fetchall()

        conn.close()

        return {
            "window_days": int(days),
            "daily_appointments": [dict(r) for r in daily_appointments],
            "daily_revenue": [dict(r) for r in daily_revenue],
            "appointment_status": [dict(r) for r in appointment_status],
            "triage_priority": [dict(r) for r in triage_priority],
            "top_doctor_workload": [dict(r) for r in top_doctor_workload],
        }

    def add_triage_case(self, data, assigned_nurse_id=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO triage_queue (patient_id, priority, symptoms, status, assigned_nurse_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                data["patient_id"],
                data.get("priority", "Medium"),
                data.get("symptoms"),
                data.get("status", "Waiting"),
                assigned_nurse_id,
            ),
        )
        conn.commit()
        triage_id = cursor.lastrowid
        conn.close()
        return triage_id

    def get_triage_cases(self):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT t.*, p.first_name AS patient_first, p.last_name AS patient_last,
                   u.full_name AS nurse_name
            FROM triage_queue t
            JOIN patients p ON t.patient_id = p.id
            LEFT JOIN users u ON t.assigned_nurse_id = u.id
            ORDER BY
                CASE t.priority
                    WHEN 'Critical' THEN 1
                    WHEN 'High' THEN 2
                    WHEN 'Medium' THEN 3
                    ELSE 4
                END,
                t.created_at DESC
            """
        ).fetchall()
        conn.close()
        return rows

    def update_triage_status(self, triage_id, status):
        conn = self.get_connection()
        conn.execute(
            """
            UPDATE triage_queue
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, triage_id),
        )
        conn.commit()
        conn.close()

    def add_nursing_note(self, data, nurse_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO nursing_notes (
                patient_id, nurse_id, blood_pressure, temperature, pulse,
                respiratory_rate, oxygen_saturation, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["patient_id"],
                nurse_id,
                data.get("blood_pressure"),
                data.get("temperature"),
                data.get("pulse"),
                data.get("respiratory_rate"),
                data.get("oxygen_saturation"),
                data.get("note"),
            ),
        )
        conn.commit()
        note_id = cursor.lastrowid
        conn.close()
        return note_id

    def get_patient_nursing_notes(self, patient_id):
        conn = self.get_connection()
        rows = conn.execute(
            """
            SELECT nn.*, u.full_name AS nurse_name
            FROM nursing_notes nn
            JOIN users u ON nn.nurse_id = u.id
            WHERE nn.patient_id = ?
            ORDER BY nn.created_at DESC
            """,
            (patient_id,),
        ).fetchall()
        conn.close()
        return rows

    def add_nurse_task(self, data, assigned_nurse_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO nurse_tasks (patient_id, assigned_nurse_id, task_title, task_details, due_at, status)
            VALUES (?, ?, ?, ?, ?, 'Pending')
            """,
            (
                data.get("patient_id"),
                assigned_nurse_id,
                data["task_title"],
                data.get("task_details"),
                data.get("due_at"),
            ),
        )
        conn.commit()
        task_id = cursor.lastrowid
        conn.close()
        return task_id

    def get_nurse_tasks(self, nurse_id=None):
        conn = self.get_connection()
        if nurse_id:
            rows = conn.execute(
                """
                SELECT nt.*, p.first_name AS patient_first, p.last_name AS patient_last
                FROM nurse_tasks nt
                LEFT JOIN patients p ON nt.patient_id = p.id
                WHERE nt.assigned_nurse_id = ?
                ORDER BY nt.created_at DESC
                """,
                (nurse_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT nt.*, p.first_name AS patient_first, p.last_name AS patient_last
                FROM nurse_tasks nt
                LEFT JOIN patients p ON nt.patient_id = p.id
                ORDER BY nt.created_at DESC
                """
            ).fetchall()
        conn.close()
        return rows

    def update_nurse_task_status(self, task_id, status):
        conn = self.get_connection()
        conn.execute("UPDATE nurse_tasks SET status = ? WHERE id = ?", (status, task_id))
        conn.commit()
        conn.close()
