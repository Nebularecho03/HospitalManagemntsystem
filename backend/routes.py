from functools import wraps

from flask import Blueprint, jsonify, request
from sqlite3 import IntegrityError

from backend.database import Database

api_bp = Blueprint("api", __name__)
db = Database()


def row_to_dict(row):
    return dict(row) if row else None


def parse_token():
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header.split(" ", 1)[1].strip()
    return None


def require_auth(roles=None):
    roles = roles or []

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            token = parse_token()
            if not token:
                return jsonify({"error": "Missing authorization token"}), 401

            user = db.get_user_by_token(token)
            if not user:
                return jsonify({"error": "Invalid or expired token"}), 401

            if roles and user["role"] not in roles:
                return jsonify({"error": "Insufficient role permission"}), 403

            request.current_user = user
            return func(*args, **kwargs)

        return wrapper

    return decorator


def get_json(required_keys=None):
    payload = request.get_json(silent=True) or {}
    missing = [key for key in (required_keys or []) if key not in payload or payload[key] in (None, "")]
    if missing:
        return None, jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400
    return payload, None, None


@api_bp.route("/auth/login", methods=["POST"])
def login():
    data, error, status = get_json(["username", "password"])
    if error:
        return error, status

    user = db.authenticate_user(data["username"], data["password"])
    if not user:
        return jsonify({"error": "Invalid username or password"}), 401

    token, expires_at = db.create_token(user["id"])
    db.log_audit(user["id"], "LOGIN", "users", user["id"], "User authenticated")

    return jsonify(
        {
            "token": token,
            "expires_at": expires_at,
            "user": row_to_dict(user),
        }
    )


@api_bp.route("/auth/register", methods=["POST"])
@require_auth(["Admin"])
def register_user():
    data, error, status = get_json(["username", "password", "role"])
    if error:
        return error, status

    try:
        user_id = db.create_user(
            username=data["username"],
            password=data["password"],
            role=data["role"],
            full_name=data.get("full_name"),
            email=data.get("email"),
        )
    except IntegrityError:
        return jsonify({"error": "Username already exists"}), 409

    db.log_audit(request.current_user["id"], "CREATE_USER", "users", user_id, f"role={data['role']}")
    return jsonify({"id": user_id, "message": "User created"}), 201


@api_bp.route("/auth/me", methods=["GET"])
@require_auth()
def me():
    return jsonify(row_to_dict(request.current_user))


@api_bp.route("/patients", methods=["GET"])
@require_auth()
def get_patients():
    filters = {
        "name": request.args.get("name"),
        "phone": request.args.get("phone"),
        "min_age": int(request.args["min_age"]) if request.args.get("min_age") else None,
        "max_age": int(request.args["max_age"]) if request.args.get("max_age") else None,
        "last_visit_days": int(request.args["last_visit_days"]) if request.args.get("last_visit_days") else None,
    }
    rows = db.get_all_patients(filters)
    return jsonify([row_to_dict(r) for r in rows])


@api_bp.route("/patients/<int:patient_id>", methods=["GET"])
@require_auth()
def get_patient(patient_id):
    row = db.get_patient(patient_id)
    if not row:
        return jsonify({"error": "Patient not found"}), 404
    return jsonify(row_to_dict(row))


@api_bp.route("/patients", methods=["POST"])
@require_auth(["Admin", "Receptionist", "Doctor", "Nurse"])
def add_patient():
    data, error, status = get_json(["first_name", "last_name"])
    if error:
        return error, status

    patient_id = db.add_patient(data)
    db.log_audit(request.current_user["id"], "CREATE_PATIENT", "patients", patient_id, data.get("email"))
    return jsonify({"id": patient_id, "message": "Patient added successfully"}), 201


@api_bp.route("/patients/<int:patient_id>", methods=["PUT"])
@require_auth(["Admin", "Receptionist", "Doctor", "Nurse"])
def update_patient(patient_id):
    if not db.get_patient(patient_id):
        return jsonify({"error": "Patient not found"}), 404

    data, error, status = get_json(["first_name", "last_name"])
    if error:
        return error, status

    db.update_patient(patient_id, data)
    db.log_audit(request.current_user["id"], "UPDATE_PATIENT", "patients", patient_id)
    return jsonify({"message": "Patient updated successfully"})


@api_bp.route("/patients/<int:patient_id>", methods=["DELETE"])
@require_auth(["Admin", "Receptionist"])
def delete_patient(patient_id):
    if not db.get_patient(patient_id):
        return jsonify({"error": "Patient not found"}), 404

    db.delete_patient(patient_id)
    db.log_audit(request.current_user["id"], "DELETE_PATIENT", "patients", patient_id)
    return jsonify({"message": "Patient deleted successfully"})


@api_bp.route("/doctors", methods=["GET"])
@require_auth()
def get_doctors():
    rows = db.get_all_doctors()
    return jsonify([row_to_dict(r) for r in rows])


@api_bp.route("/doctors", methods=["POST"])
@require_auth(["Admin", "Receptionist"])
def add_doctor():
    data, error, status = get_json(["first_name", "last_name"])
    if error:
        return error, status

    doctor_id = db.add_doctor(data)
    db.log_audit(request.current_user["id"], "CREATE_DOCTOR", "doctors", doctor_id)
    return jsonify({"id": doctor_id, "message": "Doctor added successfully"}), 201


@api_bp.route("/doctors/<int:doctor_id>/slots", methods=["POST"])
@require_auth(["Admin", "Receptionist", "Doctor", "Nurse"])
def add_doctor_slot(doctor_id):
    if not db.get_doctor(doctor_id):
        return jsonify({"error": "Doctor not found"}), 404

    data, error, status = get_json(["slot_date", "slot_time"])
    if error:
        return error, status

    try:
        slot_id = db.add_doctor_slot(doctor_id, data["slot_date"], data["slot_time"])
    except IntegrityError:
        return jsonify({"error": "Slot already exists for this doctor"}), 409

    db.log_audit(request.current_user["id"], "CREATE_DOCTOR_SLOT", "doctor_slots", slot_id)
    return jsonify({"id": slot_id, "message": "Doctor slot created"}), 201


@api_bp.route("/doctors/<int:doctor_id>/slots", methods=["GET"])
@require_auth()
def get_doctor_slots(doctor_id):
    rows = db.get_doctor_slots(doctor_id)
    return jsonify([row_to_dict(r) for r in rows])


@api_bp.route("/appointments", methods=["GET"])
@require_auth()
def get_appointments():
    rows = db.get_all_appointments()
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["patient_name"] = f"{row['patient_first']} {row['patient_last']}"
        item["doctor_name"] = f"Dr. {row['doctor_first']} {row['doctor_last']}"
        result.append(item)
    return jsonify(result)


@api_bp.route("/appointments", methods=["POST"])
@require_auth(["Admin", "Receptionist", "Doctor"])
def add_appointment():
    data, error, status = get_json(["patient_id", "doctor_id", "appointment_date", "appointment_time"])
    if error:
        return error, status

    try:
        appointment_id = db.add_appointment(data)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409

    db.log_audit(request.current_user["id"], "CREATE_APPOINTMENT", "appointments", appointment_id)
    return jsonify({"id": appointment_id, "message": "Appointment scheduled successfully"}), 201


@api_bp.route("/appointments/<int:appointment_id>/status", methods=["PUT"])
@require_auth(["Admin", "Receptionist", "Doctor", "Nurse"])
def update_appointment_status(appointment_id):
    data, error, status = get_json(["status"])
    if error:
        return error, status

    db.update_appointment_status(appointment_id, data["status"])
    db.log_audit(request.current_user["id"], "UPDATE_APPOINTMENT_STATUS", "appointments", appointment_id, data["status"])
    return jsonify({"message": "Appointment status updated successfully"})


@api_bp.route("/medical-records", methods=["POST"])
@require_auth(["Admin", "Doctor"])
def add_medical_record():
    data, error, status = get_json(["patient_id", "doctor_id"])
    if error:
        return error, status

    record_id = db.add_medical_record(data)
    db.log_audit(request.current_user["id"], "CREATE_MEDICAL_RECORD", "medical_records", record_id)
    return jsonify({"id": record_id, "message": "Medical record added successfully"}), 201


@api_bp.route("/patients/<int:patient_id>/medical-records", methods=["GET"])
@require_auth()
def get_patient_medical_records(patient_id):
    rows = db.get_patient_records(patient_id)
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["doctor_name"] = f"Dr. {row['doctor_first']} {row['doctor_last']}"
        result.append(item)
    return jsonify(result)


@api_bp.route("/prescriptions", methods=["POST"])
@require_auth(["Admin", "Doctor"])
def add_prescription():
    data, error, status = get_json(["patient_id", "doctor_id", "medication_name"])
    if error:
        return error, status

    prescription_id = db.add_prescription(data)
    db.log_audit(request.current_user["id"], "CREATE_PRESCRIPTION", "prescriptions", prescription_id)
    return jsonify({"id": prescription_id, "message": "Prescription added"}), 201


@api_bp.route("/patients/<int:patient_id>/prescriptions", methods=["GET"])
@require_auth()
def get_patient_prescriptions(patient_id):
    rows = db.get_patient_prescriptions(patient_id)
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["doctor_name"] = f"Dr. {row['doctor_first']} {row['doctor_last']}"
        result.append(item)
    return jsonify(result)


@api_bp.route("/lab-tests", methods=["POST"])
@require_auth(["Admin", "Doctor"])
def add_lab_test():
    data, error, status = get_json(["patient_id", "doctor_id", "test_name"])
    if error:
        return error, status

    test_id = db.add_lab_test(data)
    db.log_audit(request.current_user["id"], "CREATE_LAB_TEST", "lab_tests", test_id)
    return jsonify({"id": test_id, "message": "Lab test ordered"}), 201


@api_bp.route("/lab-tests/<int:test_id>/result", methods=["PUT"])
@require_auth(["Admin", "Doctor"])
def update_lab_result(test_id):
    data, _, _ = get_json([])
    db.update_lab_result(test_id, data.get("result_text"), data.get("result_file_url"))
    db.log_audit(request.current_user["id"], "UPDATE_LAB_RESULT", "lab_tests", test_id)
    return jsonify({"message": "Lab result updated"})


@api_bp.route("/patients/<int:patient_id>/lab-tests", methods=["GET"])
@require_auth()
def get_patient_lab_tests(patient_id):
    rows = db.get_patient_lab_tests(patient_id)
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["doctor_name"] = f"Dr. {row['doctor_first']} {row['doctor_last']}"
        result.append(item)
    return jsonify(result)


@api_bp.route("/attachments", methods=["POST"])
@require_auth(["Admin", "Doctor", "Receptionist", "Nurse"])
def add_attachment():
    data, error, status = get_json(["patient_id", "title", "file_url"])
    if error:
        return error, status

    attachment_id = db.add_attachment(data, uploaded_by=request.current_user["id"])
    db.log_audit(request.current_user["id"], "UPLOAD_ATTACHMENT", "attachments", attachment_id)
    return jsonify({"id": attachment_id, "message": "Attachment added"}), 201


@api_bp.route("/patients/<int:patient_id>/attachments", methods=["GET"])
@require_auth()
def get_patient_attachments(patient_id):
    rows = db.get_patient_attachments(patient_id)
    return jsonify([row_to_dict(r) for r in rows])


@api_bp.route("/reminders", methods=["POST"])
@require_auth(["Admin", "Receptionist", "Doctor", "Nurse"])
def create_reminder():
    data, error, status = get_json(["patient_id", "channel", "message"])
    if error:
        return error, status

    reminder_id = db.create_reminder(data)
    db.log_audit(request.current_user["id"], "QUEUE_REMINDER", "reminders", reminder_id)
    return jsonify({"id": reminder_id, "message": "Reminder queued"}), 201


@api_bp.route("/reminders", methods=["GET"])
@require_auth()
def get_reminders():
    rows = db.get_reminders()
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["patient_name"] = f"{row['patient_first']} {row['patient_last']}"
        result.append(item)
    return jsonify(result)


@api_bp.route("/invoices", methods=["POST"])
@require_auth(["Admin", "Receptionist"])
def create_invoice():
    data, error, status = get_json(["patient_id"])
    if error:
        return error, status

    invoice_id = db.add_invoice(data)
    db.log_audit(request.current_user["id"], "CREATE_INVOICE", "invoices", invoice_id)
    return jsonify({"id": invoice_id, "message": "Invoice created"}), 201


@api_bp.route("/invoices", methods=["GET"])
@require_auth()
def get_invoices():
    rows = db.get_invoices()
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["patient_name"] = f"{row['patient_first']} {row['patient_last']}"
        result.append(item)
    return jsonify(result)


@api_bp.route("/invoices/<int:invoice_id>/pay", methods=["PUT"])
@require_auth(["Admin", "Receptionist"])
def pay_invoice(invoice_id):
    db.mark_invoice_paid(invoice_id)
    db.log_audit(request.current_user["id"], "PAY_INVOICE", "invoices", invoice_id)
    return jsonify({"message": "Invoice marked as paid"})


@api_bp.route("/patients/<int:patient_id>/invoices", methods=["GET"])
@require_auth()
def get_patient_invoices(patient_id):
    rows = db.get_patient_invoices(patient_id)
    return jsonify([row_to_dict(r) for r in rows])


@api_bp.route("/audit-logs", methods=["GET"])
@require_auth(["Admin"])
def audit_logs():
    limit = int(request.args.get("limit", 200))
    rows = db.get_audit_logs(limit)
    return jsonify([row_to_dict(r) for r in rows])


@api_bp.route("/stats", methods=["GET"])
@require_auth()
def get_statistics():
    return jsonify(db.get_dashboard_stats())


@api_bp.route("/analytics", methods=["GET"])
@require_auth(["Admin"])
def get_admin_analytics():
    days = int(request.args.get("days", 14))
    days = max(1, min(days, 90))
    return jsonify(db.get_admin_analytics(days=days))


@api_bp.route("/triage", methods=["GET"])
@require_auth(["Admin", "Doctor", "Receptionist", "Nurse"])
def get_triage_cases():
    rows = db.get_triage_cases()
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["patient_name"] = f"{row['patient_first']} {row['patient_last']}"
        result.append(item)
    return jsonify(result)


@api_bp.route("/triage", methods=["POST"])
@require_auth(["Admin", "Receptionist", "Nurse"])
def add_triage_case():
    data, error, status = get_json(["patient_id", "priority"])
    if error:
        return error, status

    assigned_nurse_id = request.current_user["id"] if request.current_user["role"] == "Nurse" else None
    triage_id = db.add_triage_case(data, assigned_nurse_id=assigned_nurse_id)
    db.log_audit(request.current_user["id"], "CREATE_TRIAGE_CASE", "triage_queue", triage_id, data.get("priority"))
    return jsonify({"id": triage_id, "message": "Triage case added"}), 201


@api_bp.route("/triage/<int:triage_id>/status", methods=["PUT"])
@require_auth(["Admin", "Doctor", "Nurse"])
def update_triage_case_status(triage_id):
    data, error, status = get_json(["status"])
    if error:
        return error, status

    db.update_triage_status(triage_id, data["status"])
    db.log_audit(request.current_user["id"], "UPDATE_TRIAGE_STATUS", "triage_queue", triage_id, data["status"])
    return jsonify({"message": "Triage status updated"})


@api_bp.route("/nursing-notes", methods=["POST"])
@require_auth(["Admin", "Nurse"])
def add_nursing_note():
    data, error, status = get_json(["patient_id"])
    if error:
        return error, status

    nurse_id = request.current_user["id"]
    note_id = db.add_nursing_note(data, nurse_id=nurse_id)
    db.log_audit(request.current_user["id"], "CREATE_NURSING_NOTE", "nursing_notes", note_id)
    return jsonify({"id": note_id, "message": "Nursing note added"}), 201


@api_bp.route("/patients/<int:patient_id>/nursing-notes", methods=["GET"])
@require_auth(["Admin", "Doctor", "Nurse"])
def get_patient_nursing_notes(patient_id):
    rows = db.get_patient_nursing_notes(patient_id)
    return jsonify([row_to_dict(r) for r in rows])


@api_bp.route("/nurse-tasks", methods=["POST"])
@require_auth(["Admin", "Receptionist", "Nurse"])
def add_nurse_task():
    data, error, status = get_json(["task_title"])
    if error:
        return error, status

    assigned_nurse_id = data.get("assigned_nurse_id") or request.current_user["id"]
    task_id = db.add_nurse_task(data, assigned_nurse_id=assigned_nurse_id)
    db.log_audit(request.current_user["id"], "CREATE_NURSE_TASK", "nurse_tasks", task_id)
    return jsonify({"id": task_id, "message": "Nurse task created"}), 201


@api_bp.route("/nurse-tasks", methods=["GET"])
@require_auth(["Admin", "Nurse"])
def get_nurse_tasks():
    nurse_id = request.current_user["id"] if request.current_user["role"] == "Nurse" else None
    rows = db.get_nurse_tasks(nurse_id=nurse_id)
    result = []
    for row in rows:
        item = row_to_dict(row)
        if row["patient_first"]:
            item["patient_name"] = f"{row['patient_first']} {row['patient_last']}"
        else:
            item["patient_name"] = None
        result.append(item)
    return jsonify(result)


@api_bp.route("/nurse-tasks/<int:task_id>/status", methods=["PUT"])
@require_auth(["Admin", "Nurse"])
def update_nurse_task_status(task_id):
    data, error, status = get_json(["status"])
    if error:
        return error, status

    db.update_nurse_task_status(task_id, data["status"])
    db.log_audit(request.current_user["id"], "UPDATE_NURSE_TASK_STATUS", "nurse_tasks", task_id, data["status"])
    return jsonify({"message": "Nurse task status updated"})
