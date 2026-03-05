import requests
import json

BASE_URL = "http://localhost:5000/api"


def test_health():
    response = requests.get(f"{BASE_URL}/health")
    print("Health Check:", response.json())


def test_add_patient():
    patient = {
        "first_name": "John",
        "last_name": "Doe",
        "age": 35,
        "gender": "Male",
        "phone": "1234567890",
        "email": "john@example.com",
        "address": "123 Main St"
    }
    response = requests.post(f"{BASE_URL}/patients", json=patient)
    assert response.status_code == 201
    data = response.json()
    print("Add Patient:", data)
    assert data.get('id')
    return data.get('id')


def test_get_patients():
    response = requests.get(f"{BASE_URL}/patients")
    print("Get Patients:", len(response.json()), "patients found")


def test_add_doctor():
    doctor = {
        "first_name": "Jane",
        "last_name": "Smith",
        "specialization": "Cardiology",
        "phone": "9876543210",
        "email": "jane.smith@hospital.com",
        "available_days": "Mon-Fri 9AM-5PM"
    }
    response = requests.post(f"{BASE_URL}/doctors", json=doctor)
    assert response.status_code == 201
    data = response.json()
    print("Add Doctor:", data)
    assert data.get('id')
    return data.get('id')

def test_add_appointment(patient_id, doctor_id):
    # schedule an appointment for tomorrow
    from datetime import datetime, timedelta
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    appointment = {
        "patient_id": patient_id,
        "doctor_id": doctor_id,
        "appointment_date": tomorrow,
        "appointment_time": "10:00",
        "reason": "Routine checkup"
    }
    response = requests.post(f"{BASE_URL}/appointments", json=appointment)
    print("Add Appointment:", response.json())
    return response.json().get('id')

def test_get_reminders():
    response = requests.get(f"{BASE_URL}/appointments/reminders?days=2")
    print("Upcoming Reminders:", response.json())



def test_stats():
    response = requests.get(f"{BASE_URL}/stats")
    print("Statistics:", response.json())


if __name__ == "__main__":
    print("Testing Hospital Management System API...")
    print("-" * 40)

    test_health()
    print("-" * 40)

    patient_id = test_add_patient()
    print("-" * 40)

    test_get_patients()
    print("-" * 40)

    doctor_id = test_add_doctor()
    print("-" * 40)

    test_stats()
    appointment_id = test_add_appointment(patient_id, doctor_id)
    print("-" * 40)
    test_get_reminders()

    # new flow: doctor registers a new patient directly and we verify
    new_patient_id = None
    response = requests.post(f"{BASE_URL}/doctors/{doctor_id}/patients", json={
        "first_name": "Alice",
        "last_name": "Brown",
        "age": 28,
        "gender": "Female"
    })
    print("Doctor registered patient:", response.json())
    assert response.status_code == 201
    new_patient_id = response.json().get('id')
    assert new_patient_id
    # now fetch doctor patients
    resp2 = requests.get(f"{BASE_URL}/doctors/{doctor_id}/patients")
    assert resp2.status_code == 200
    patients_list = resp2.json()
    assert any(p['id'] == new_patient_id for p in patients_list)
    print("Doctor has patients count:", len(patients_list))
    print("-" * 40)

    print("-" * 40)

    print("✅ API tests completed!")