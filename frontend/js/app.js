const API_BASE = '/api';

const PORTAL_ROLE_RULES = {
    admin: ['Admin'],
    clinical: ['Doctor', 'Nurse'],
    frontdesk: ['Receptionist']
};

const ROLE_SECTION_ACCESS = {
    Admin: ['dashboard', 'patients', 'doctors', 'appointments', 'clinical', 'labs', 'billing', 'reminders', 'nurse-station', 'users', 'audit'],
    Doctor: ['dashboard', 'patients', 'appointments', 'clinical', 'labs', 'reminders', 'nurse-station'],
    Nurse: ['dashboard', 'patients', 'appointments', 'reminders', 'nurse-station'],
    Receptionist: ['dashboard', 'patients', 'doctors', 'appointments', 'billing', 'reminders']
};

let authToken = localStorage.getItem('hms_token') || '';
let currentUser = null;
let cache = {
    patients: [],
    doctors: [],
    appointments: [],
    invoices: [],
    reminders: [],
    triage: [],
    nurseTasks: [],
    analytics: null
};
let analyticsInterval = null;

function setStatus(message, isError = false) {
    const el = document.getElementById('global-status');
    if (!el) return;
    el.textContent = message || '';
    el.style.color = isError ? '#c94040' : '#05534b';
}

function setAuthStatus(message, isError = true) {
    const el = document.getElementById('auth-status');
    if (!el) return;
    el.textContent = message || '';
    el.style.color = isError ? '#c94040' : '#05534b';
}

async function api(path, options = {}) {
    const headers = options.headers || {};
    headers['Content-Type'] = 'application/json';
    if (authToken) headers['Authorization'] = `Bearer ${authToken}`;

    const response = await fetch(`${API_BASE}${path}`, { ...options, headers });
    let payload = {};
    try {
        payload = await response.json();
    } catch {
        payload = {};
    }

    if (!response.ok) {
        throw new Error(payload.error || `Request failed (${response.status})`);
    }
    return payload;
}

function allowedSections() {
    if (!currentUser) return [];
    return ROLE_SECTION_ACCESS[currentUser.role] || [];
}

function applyRoleAccess() {
    const allowed = new Set(allowedSections());

    document.querySelectorAll('.nav-btn').forEach((btn) => {
        const section = btn.dataset.section;
        const roleRule = btn.dataset.roles;
        let visible = allowed.has(section);
        if (visible && roleRule) {
            const roles = roleRule.split(',').map((r) => r.trim());
            visible = roles.includes(currentUser.role);
        }
        btn.style.display = visible ? '' : 'none';
    });

    document.querySelectorAll('[data-roles]').forEach((el) => {
        if (el.classList.contains('nav-btn')) return;
        const allowedRoles = (el.dataset.roles || '')
            .split(',')
            .map((r) => r.trim())
            .filter(Boolean);
        const visible = allowedRoles.length === 0 || allowedRoles.includes(currentUser.role);
        el.style.display = visible ? '' : 'none';
    });
}

function showSection(section) {
    if (!allowedSections().includes(section)) {
        setStatus('You do not have access to this section.', true);
        return;
    }

    document.querySelectorAll('.panel').forEach((panel) => panel.classList.remove('active'));
    const panel = document.getElementById(section);
    if (panel) panel.classList.add('active');

    document.querySelectorAll('.nav-btn').forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.section === section);
    });

    document.getElementById('section-title').textContent =
        section.replace('-', ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

function optionList(items, labelBuilder) {
    return items.map((item) => `<option value="${item.id}">${labelBuilder(item)}</option>`).join('');
}

function fillSelect(id, items, labelBuilder, includeBlank = true) {
    const el = document.getElementById(id);
    if (!el) return;
    const blank = includeBlank ? '<option value="">Select...</option>' : '';
    el.innerHTML = blank + optionList(items, labelBuilder);
}

function refreshEntitySelectors() {
    const patientLabel = (p) => `${p.first_name} ${p.last_name}`;
    const doctorLabel = (d) => `Dr. ${d.first_name} ${d.last_name}`;

    ['a-patient', 'mr-patient', 'pr-patient', 'at-patient', 'lt-patient', 'iv-patient', 'rm-patient', 'clinical-view-patient', 'lab-view-patient', 'tr-patient', 'nn-patient', 'nt-patient']
        .forEach((id) => fillSelect(id, cache.patients, patientLabel));

    ['a-doctor', 'mr-doctor', 'pr-doctor', 'lt-doctor', 'slot-doctor', 'slot-view-doctor']
        .forEach((id) => fillSelect(id, cache.doctors, doctorLabel));
}

function renderStats(stats) {
    const items = [
        ['Total Patients', stats.total_patients],
        ['Total Doctors', stats.total_doctors],
        ['Today Appointments', stats.today_appointments],
        ['Pending Appointments', stats.pending_appointments],
        ['Open Triage Cases', stats.open_triage_cases || 0],
        ['Open Nurse Tasks', stats.open_nurse_tasks || 0],
        ['No-show Rate', `${stats.no_show_rate}%`],
        ['Paid Revenue', `$${Number(stats.paid_revenue || 0).toFixed(2)}`]
    ];

    document.getElementById('stats-cards').innerHTML = items
        .map(([title, value]) => `<article class="kpi"><h4>${title}</h4><p>${value}</p></article>`)
        .join('');
}

function renderPatients() {
    document.getElementById('patients-table').innerHTML = cache.patients
        .map((p) => `<tr><td>${p.id}</td><td>${p.first_name} ${p.last_name}</td><td>${p.age ?? '-'}</td><td>${p.phone || '-'}</td><td>${p.email || '-'}</td><td>${p.last_visit || '-'}</td></tr>`)
        .join('');
}

function renderDoctors() {
    document.getElementById('doctors-table').innerHTML = cache.doctors
        .map((d) => `<tr><td>${d.id}</td><td>Dr. ${d.first_name} ${d.last_name}</td><td>${d.specialization || '-'}</td><td>${d.phone || '-'}</td><td>${d.email || '-'}</td></tr>`)
        .join('');
}

function renderAppointments() {
    const badge = (status) => `badge-${String(status || '').toLowerCase()}`;
    document.getElementById('appointments-table').innerHTML = cache.appointments
        .map((a) => `<tr><td>${a.id}</td><td>${a.patient_name}</td><td>${a.doctor_name}</td><td>${a.appointment_date}</td><td>${a.appointment_time}</td><td><span class="badge ${badge(a.status)}">${a.status}</span></td><td>${a.reason || '-'}</td></tr>`)
        .join('');
}

function renderInvoices() {
    document.getElementById('invoices-table').innerHTML = cache.invoices
        .map((i) => `<tr><td>${i.id}</td><td>${i.patient_name || i.patient_id}</td><td>$${Number(i.total_amount).toFixed(2)}</td><td><span class="badge badge-${String(i.status).toLowerCase()}">${i.status}</span></td><td>${i.issued_at}</td></tr>`)
        .join('');
}

function renderReminders() {
    document.getElementById('reminders-list').innerHTML = cache.reminders
        .slice(0, 150)
        .map((r) => `<li>#${r.id} ${r.channel} to ${r.patient_name}: ${r.message} (${r.status})</li>`)
        .join('');
}

function renderTriage() {
    const statusOptions = ['Waiting', 'In Progress', 'Completed'];
    document.getElementById('triage-table').innerHTML = cache.triage
        .map((t) => {
            const options = statusOptions
                .map((s) => `<option value="${s}" ${s === t.status ? 'selected' : ''}>${s}</option>`)
                .join('');
            return `<tr>
                <td>${t.id}</td>
                <td>${t.patient_name}</td>
                <td>${t.priority}</td>
                <td>${t.status}</td>
                <td>${t.symptoms || '-'}</td>
                <td>
                    <select id="triage-status-${t.id}">${options}</select>
                    <button data-triage-update="${t.id}">Save</button>
                </td>
            </tr>`;
        })
        .join('');
}

function renderNurseTasks() {
    document.getElementById('nurse-tasks-list').innerHTML = cache.nurseTasks
        .map((t) => `<li>
            <strong>#${t.id}</strong> ${t.task_title} ${t.patient_name ? `for ${t.patient_name}` : ''} • ${t.status}
            <div class="inline-controls">
                <select id="task-status-${t.id}">
                    <option ${t.status === 'Pending' ? 'selected' : ''}>Pending</option>
                    <option ${t.status === 'In Progress' ? 'selected' : ''}>In Progress</option>
                    <option ${t.status === 'Done' ? 'selected' : ''}>Done</option>
                </select>
                <button data-task-update="${t.id}">Update</button>
            </div>
        </li>`)
        .join('') || '<li>No nurse tasks available.</li>';
}

function renderBarChart(elementId, rows, labelKey, valueKey) {
    const el = document.getElementById(elementId);
    if (!el) return;
    if (!rows || rows.length === 0) {
        el.innerHTML = '<div class="bar-item"><span class="label">No data</span><span class="track"><span class="fill" style="width:0%"></span></span><span class="value">0</span></div>';
        return;
    }

    const max = Math.max(...rows.map((r) => Number(r[valueKey] || 0)), 1);
    el.innerHTML = rows
        .map((r) => {
            const val = Number(r[valueKey] || 0);
            const width = Math.max(2, Math.round((val / max) * 100));
            return `<div class="bar-item">
                <span class="label">${r[labelKey]}</span>
                <span class="track"><span class="fill" style="width:${width}%"></span></span>
                <span class="value">${Number.isInteger(val) ? val : val.toFixed(2)}</span>
            </div>`;
        })
        .join('');
}

function renderAdminAnalytics(data) {
    if (!data) return;
    renderBarChart('chart-appointments', data.daily_appointments || [], 'day', 'total');
    renderBarChart('chart-revenue', data.daily_revenue || [], 'day', 'total');
    renderBarChart('chart-status', data.appointment_status || [], 'status', 'total');
    renderBarChart('chart-triage', data.triage_priority || [], 'priority', 'total');
    renderBarChart('chart-workload', data.top_doctor_workload || [], 'doctor_name', 'total');
}

async function loadAdminAnalytics() {
    if (!currentUser || currentUser.role !== 'Admin') return;
    const days = Number(document.getElementById('analytics-window')?.value || 14);
    const analytics = await api(`/analytics?days=${days}`);
    cache.analytics = analytics;
    renderAdminAnalytics(analytics);
}

function startAnalyticsPolling() {
    if (analyticsInterval) {
        clearInterval(analyticsInterval);
        analyticsInterval = null;
    }
    if (!currentUser || currentUser.role !== 'Admin') return;
    analyticsInterval = setInterval(() => {
        loadAdminAnalytics().catch(() => {});
    }, 10000);
}

async function loadCoreData(patientQuery = '') {
    const requests = [
        api('/stats'),
        api(`/patients${patientQuery}`),
        api('/doctors'),
        api('/appointments'),
        api('/invoices'),
        api('/reminders')
    ];

    const [stats, patients, doctors, appointments, invoices, reminders] = await Promise.all(requests);

    cache = { ...cache, patients, doctors, appointments, invoices, reminders };
    renderStats(stats);
    renderPatients();
    renderDoctors();
    renderAppointments();
    renderInvoices();
    renderReminders();

    if (allowedSections().includes('nurse-station')) {
        const triage = await api('/triage');
        const tasks = await api('/nurse-tasks');
        cache.triage = triage;
        cache.nurseTasks = tasks;
        renderTriage();
        renderNurseTasks();
    }

    if (currentUser && currentUser.role === 'Admin') {
        await loadAdminAnalytics();
    }

    refreshEntitySelectors();
}

async function performLogin(username, password, portalKind) {
    const result = await api('/auth/login', {
        method: 'POST',
        body: JSON.stringify({ username, password })
    });

    const allowedRoles = PORTAL_ROLE_RULES[portalKind] || [];
    if (!allowedRoles.includes(result.user.role)) {
        throw new Error(`Use the correct portal for role: ${result.user.role}`);
    }

    authToken = result.token;
    currentUser = result.user;
    localStorage.setItem('hms_token', authToken);

    document.getElementById('current-user-label').textContent = `${currentUser.full_name || currentUser.username} • ${currentUser.role}`;
    applyRoleAccess();

    document.getElementById('auth-shell').classList.add('hidden');
    document.getElementById('app-shell').classList.remove('hidden');

    await loadCoreData();
    const startSection = currentUser.role === 'Nurse' ? 'nurse-station' : currentUser.role === 'Receptionist' ? 'appointments' : 'dashboard';
    showSection(startSection);
    setStatus('Data loaded successfully.');
    setAuthStatus('', false);
    startAnalyticsPolling();
}

function bindLoginTabs() {
    document.querySelectorAll('.login-tab').forEach((btn) => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.login-tab').forEach((b) => b.classList.remove('active'));
            btn.classList.add('active');
            const target = btn.dataset.loginTarget;
            document.querySelectorAll('.login-role-form').forEach((f) => f.classList.remove('active'));
            document.getElementById(target).classList.add('active');
        });
    });
}

function bindLoginForms() {
    document.getElementById('admin-login').addEventListener('submit', async (event) => {
        event.preventDefault();
        try {
            await performLogin(
                document.getElementById('admin-login-username').value.trim(),
                document.getElementById('admin-login-password').value,
                'admin'
            );
        } catch (error) {
            setAuthStatus(error.message, true);
        }
    });

    document.getElementById('clinical-login').addEventListener('submit', async (event) => {
        event.preventDefault();
        try {
            await performLogin(
                document.getElementById('clinical-login-username').value.trim(),
                document.getElementById('clinical-login-password').value,
                'clinical'
            );
        } catch (error) {
            setAuthStatus(error.message, true);
        }
    });

    document.getElementById('frontdesk-login').addEventListener('submit', async (event) => {
        event.preventDefault();
        try {
            await performLogin(
                document.getElementById('frontdesk-login-username').value.trim(),
                document.getElementById('frontdesk-login-password').value,
                'frontdesk'
            );
        } catch (error) {
            setAuthStatus(error.message, true);
        }
    });
}

function bindNav() {
    document.querySelectorAll('.nav-btn').forEach((btn) => {
        btn.addEventListener('click', () => showSection(btn.dataset.section));
    });
}

async function submitPatient(event) {
    event.preventDefault();
    const body = {
        first_name: document.getElementById('p-first').value.trim(),
        last_name: document.getElementById('p-last').value.trim(),
        age: document.getElementById('p-age').value || null,
        gender: document.getElementById('p-gender').value || null,
        phone: document.getElementById('p-phone').value || null,
        email: document.getElementById('p-email').value || null,
        address: document.getElementById('p-address').value || null
    };
    await api('/patients', { method: 'POST', body: JSON.stringify(body) });
    await loadCoreData();
    event.target.reset();
    setStatus('Patient created.');
}

async function searchPatients(event) {
    event.preventDefault();
    const params = new URLSearchParams();
    [['name', 'search-name'], ['phone', 'search-phone'], ['min_age', 'search-min-age'], ['max_age', 'search-max-age'], ['last_visit_days', 'search-last-visit']]
        .forEach(([key, id]) => {
            const value = document.getElementById(id).value;
            if (value !== '') params.set(key, value);
        });

    const query = params.toString() ? `?${params.toString()}` : '';
    await loadCoreData(query);
    setStatus('Patient filters applied.');
}

async function submitDoctor(event) {
    event.preventDefault();
    const body = {
        first_name: document.getElementById('d-first').value.trim(),
        last_name: document.getElementById('d-last').value.trim(),
        specialization: document.getElementById('d-specialization').value || null,
        phone: document.getElementById('d-phone').value || null,
        email: document.getElementById('d-email').value || null,
        available_days: document.getElementById('d-availability').value || null
    };
    await api('/doctors', { method: 'POST', body: JSON.stringify(body) });
    await loadCoreData();
    event.target.reset();
    setStatus('Doctor added.');
}

async function submitSlot(event) {
    event.preventDefault();
    const doctorId = document.getElementById('slot-doctor').value;
    const body = {
        slot_date: document.getElementById('slot-date').value,
        slot_time: document.getElementById('slot-time').value
    };
    await api(`/doctors/${doctorId}/slots`, { method: 'POST', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('Doctor slot created.');
}

async function loadSlots() {
    const doctorId = document.getElementById('slot-view-doctor').value;
    if (!doctorId) return;
    const slots = await api(`/doctors/${doctorId}/slots`);
    document.getElementById('slots-table').innerHTML = slots
        .map((s) => `<tr><td>${s.id}</td><td>${s.slot_date}</td><td>${s.slot_time}</td><td>${s.is_available ? 'Yes' : 'No'}</td></tr>`)
        .join('');
}

async function submitAppointment(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('a-patient').value),
        doctor_id: Number(document.getElementById('a-doctor').value),
        appointment_date: document.getElementById('a-date').value,
        appointment_time: document.getElementById('a-time').value,
        reason: document.getElementById('a-reason').value || null
    };
    await api('/appointments', { method: 'POST', body: JSON.stringify(body) });
    await loadCoreData();
    event.target.reset();
    setStatus('Appointment scheduled.');
}

async function submitAppointmentStatus(event) {
    event.preventDefault();
    const id = document.getElementById('a-id').value;
    const status = document.getElementById('a-status').value;
    await api(`/appointments/${id}/status`, { method: 'PUT', body: JSON.stringify({ status }) });
    await loadCoreData();
    setStatus('Appointment status updated.');
}

async function submitMedicalRecord(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('mr-patient').value),
        doctor_id: Number(document.getElementById('mr-doctor').value),
        diagnosis: document.getElementById('mr-diagnosis').value,
        prescription: document.getElementById('mr-prescription').value,
        notes: document.getElementById('mr-notes').value
    };
    await api('/medical-records', { method: 'POST', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('Medical record added.');
}

async function submitPrescription(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('pr-patient').value),
        doctor_id: Number(document.getElementById('pr-doctor').value),
        medication_name: document.getElementById('pr-medication').value,
        dosage: document.getElementById('pr-dosage').value,
        frequency: document.getElementById('pr-frequency').value,
        duration_days: document.getElementById('pr-duration').value || null
    };
    await api('/prescriptions', { method: 'POST', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('Prescription added.');
}

async function submitAttachment(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('at-patient').value),
        title: document.getElementById('at-title').value,
        file_url: document.getElementById('at-url').value
    };
    await api('/attachments', { method: 'POST', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('Attachment added.');
}

async function loadClinicalHistory() {
    const patientId = document.getElementById('clinical-view-patient').value;
    if (!patientId) return;

    const [records, prescriptions, attachments, nursingNotes] = await Promise.all([
        api(`/patients/${patientId}/medical-records`),
        api(`/patients/${patientId}/prescriptions`),
        api(`/patients/${patientId}/attachments`),
        api(`/patients/${patientId}/nursing-notes`).catch(() => [])
    ]);

    const nurseItems = nursingNotes.map((n) => `${n.created_at} • ${n.nurse_name || 'Nurse'} • BP:${n.blood_pressure || '-'} Temp:${n.temperature || '-'} Pulse:${n.pulse || '-'}`);
    document.getElementById('records-list').innerHTML = [
        ...records.map((r) => `${r.visit_date || '-'} • ${r.doctor_name} • ${r.diagnosis || 'No diagnosis'}`),
        ...nurseItems
    ].map((txt) => `<li>${txt}</li>`).join('') || '<li>No clinical records found.</li>';

    document.getElementById('prescriptions-list').innerHTML = prescriptions
        .map((p) => `<li>${p.created_at} • ${p.medication_name} (${p.dosage || '-'}) by ${p.doctor_name}</li>`)
        .join('') || '<li>No prescriptions found.</li>';

    document.getElementById('attachments-list').innerHTML = attachments
        .map((a) => `<li>${a.uploaded_at} • <a href="${a.file_url}" target="_blank" rel="noopener">${a.title}</a></li>`)
        .join('') || '<li>No attachments found.</li>';
}

async function submitLabOrder(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('lt-patient').value),
        doctor_id: Number(document.getElementById('lt-doctor').value),
        test_name: document.getElementById('lt-name').value
    };
    await api('/lab-tests', { method: 'POST', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('Lab test ordered.');
}

async function submitLabResult(event) {
    event.preventDefault();
    const id = document.getElementById('lr-id').value;
    const body = {
        result_text: document.getElementById('lr-text').value,
        result_file_url: document.getElementById('lr-file').value
    };
    await api(`/lab-tests/${id}/result`, { method: 'PUT', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('Lab result updated.');
}

async function loadLabHistory() {
    const patientId = document.getElementById('lab-view-patient').value;
    if (!patientId) return;
    const tests = await api(`/patients/${patientId}/lab-tests`);
    document.getElementById('labs-list').innerHTML = tests
        .map((t) => `<li>#${t.id} ${t.test_name} • ${t.status} • ${t.doctor_name} • ${t.ordered_at}</li>`)
        .join('') || '<li>No lab tests found.</li>';
}

async function submitInvoice(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('iv-patient').value),
        consultation_fee: Number(document.getElementById('iv-consult').value || 0),
        lab_fee: Number(document.getElementById('iv-lab').value || 0),
        pharmacy_fee: Number(document.getElementById('iv-pharmacy').value || 0)
    };
    await api('/invoices', { method: 'POST', body: JSON.stringify(body) });
    await loadCoreData();
    event.target.reset();
    setStatus('Invoice created.');
}

async function payInvoice(event) {
    event.preventDefault();
    const id = document.getElementById('iv-pay-id').value;
    await api(`/invoices/${id}/pay`, { method: 'PUT' });
    await loadCoreData();
    event.target.reset();
    setStatus('Invoice marked paid.');
}

async function submitReminder(event) {
    event.preventDefault();
    const appointmentValue = document.getElementById('rm-appointment').value;
    const scheduledForValue = document.getElementById('rm-when').value;
    const body = {
        patient_id: Number(document.getElementById('rm-patient').value),
        appointment_id: appointmentValue ? Number(appointmentValue) : null,
        channel: document.getElementById('rm-channel').value,
        message: document.getElementById('rm-message').value,
        scheduled_for: scheduledForValue ? scheduledForValue.replace('T', ' ') : null
    };
    await api('/reminders', { method: 'POST', body: JSON.stringify(body) });
    await loadCoreData();
    event.target.reset();
    setStatus('Reminder queued.');
}

async function submitUser(event) {
    event.preventDefault();
    const body = {
        username: document.getElementById('u-username').value,
        password: document.getElementById('u-password').value,
        role: document.getElementById('u-role').value,
        full_name: document.getElementById('u-fullname').value,
        email: document.getElementById('u-email').value
    };
    await api('/auth/register', { method: 'POST', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('User account created.');
}

async function loadAudit() {
    const logs = await api('/audit-logs?limit=250');
    document.getElementById('audit-table').innerHTML = logs
        .map((l) => `<tr><td>${l.created_at}</td><td>${l.username || '-'}</td><td>${l.role || '-'}</td><td>${l.action}</td><td>${l.entity_type || '-'}</td><td>${l.entity_id || '-'}</td><td>${l.details || '-'}</td></tr>`)
        .join('');
}

async function submitTriage(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('tr-patient').value),
        priority: document.getElementById('tr-priority').value,
        symptoms: document.getElementById('tr-symptoms').value || null
    };
    await api('/triage', { method: 'POST', body: JSON.stringify(body) });
    await loadCoreData();
    event.target.reset();
    setStatus('Triage case created.');
}

async function submitNursingNote(event) {
    event.preventDefault();
    const body = {
        patient_id: Number(document.getElementById('nn-patient').value),
        blood_pressure: document.getElementById('nn-bp').value || null,
        temperature: document.getElementById('nn-temp').value || null,
        pulse: document.getElementById('nn-pulse').value || null,
        respiratory_rate: document.getElementById('nn-rr').value || null,
        oxygen_saturation: document.getElementById('nn-spo2').value || null,
        note: document.getElementById('nn-note').value || null
    };
    await api('/nursing-notes', { method: 'POST', body: JSON.stringify(body) });
    event.target.reset();
    setStatus('Nursing note saved.');
}

async function submitNurseTask(event) {
    event.preventDefault();
    const due = document.getElementById('nt-due').value;
    const body = {
        patient_id: Number(document.getElementById('nt-patient').value) || null,
        task_title: document.getElementById('nt-title').value,
        task_details: document.getElementById('nt-details').value || null,
        due_at: due ? due.replace('T', ' ') : null
    };
    await api('/nurse-tasks', { method: 'POST', body: JSON.stringify(body) });
    await loadCoreData();
    event.target.reset();
    setStatus('Nurse task created.');
}

async function updateTriageStatus(triageId) {
    const status = document.getElementById(`triage-status-${triageId}`).value;
    await api(`/triage/${triageId}/status`, { method: 'PUT', body: JSON.stringify({ status }) });
    await loadCoreData();
    setStatus('Triage status updated.');
}

async function updateNurseTaskStatus(taskId) {
    const status = document.getElementById(`task-status-${taskId}`).value;
    await api(`/nurse-tasks/${taskId}/status`, { method: 'PUT', body: JSON.stringify({ status }) });
    await loadCoreData();
    setStatus('Nurse task status updated.');
}

function bindDynamicActions() {
    document.addEventListener('click', (event) => {
        const triageButton = event.target.closest('[data-triage-update]');
        if (triageButton) {
            wrap(() => updateTriageStatus(triageButton.dataset.triageUpdate))();
            return;
        }

        const taskButton = event.target.closest('[data-task-update]');
        if (taskButton) {
            wrap(() => updateNurseTaskStatus(taskButton.dataset.taskUpdate))();
        }
    });
}

function bindForms() {
    document.getElementById('patient-form').addEventListener('submit', wrap(submitPatient));
    document.getElementById('patient-search-form').addEventListener('submit', wrap(searchPatients));
    document.getElementById('doctor-form').addEventListener('submit', wrap(submitDoctor));
    document.getElementById('slot-form').addEventListener('submit', wrap(submitSlot));
    document.getElementById('load-slots-btn').addEventListener('click', wrap(loadSlots));

    document.getElementById('appointment-form').addEventListener('submit', wrap(submitAppointment));
    document.getElementById('appointment-status-form').addEventListener('submit', wrap(submitAppointmentStatus));

    document.getElementById('record-form').addEventListener('submit', wrap(submitMedicalRecord));
    document.getElementById('prescription-form').addEventListener('submit', wrap(submitPrescription));
    document.getElementById('attachment-form').addEventListener('submit', wrap(submitAttachment));
    document.getElementById('load-clinical-btn').addEventListener('click', wrap(loadClinicalHistory));

    document.getElementById('lab-order-form').addEventListener('submit', wrap(submitLabOrder));
    document.getElementById('lab-result-form').addEventListener('submit', wrap(submitLabResult));
    document.getElementById('load-labs-btn').addEventListener('click', wrap(loadLabHistory));

    document.getElementById('invoice-form').addEventListener('submit', wrap(submitInvoice));
    document.getElementById('invoice-pay-form').addEventListener('submit', wrap(payInvoice));

    document.getElementById('reminder-form').addEventListener('submit', wrap(submitReminder));

    document.getElementById('user-form').addEventListener('submit', wrap(submitUser));
    document.getElementById('refresh-audit').addEventListener('click', wrap(loadAudit));

    document.getElementById('triage-form').addEventListener('submit', wrap(submitTriage));
    document.getElementById('nursing-note-form').addEventListener('submit', wrap(submitNursingNote));
    document.getElementById('nurse-task-form').addEventListener('submit', wrap(submitNurseTask));

    const refreshAnalytics = document.getElementById('refresh-analytics');
    if (refreshAnalytics) refreshAnalytics.addEventListener('click', wrap(loadAdminAnalytics));
    const analyticsWindow = document.getElementById('analytics-window');
    if (analyticsWindow) analyticsWindow.addEventListener('change', wrap(loadAdminAnalytics));
}

function wrap(fn) {
    return async (event) => {
        try {
            await fn(event);
        } catch (error) {
            setStatus(error.message, true);
        }
    };
}

function logout() {
    authToken = '';
    currentUser = null;
    localStorage.removeItem('hms_token');
    if (analyticsInterval) {
        clearInterval(analyticsInterval);
        analyticsInterval = null;
    }
    document.getElementById('app-shell').classList.add('hidden');
    document.getElementById('auth-shell').classList.remove('hidden');
    setStatus('');
}

async function bootstrapFromToken() {
    if (!authToken) return;
    try {
        currentUser = await api('/auth/me');
        document.getElementById('current-user-label').textContent = `${currentUser.full_name || currentUser.username} • ${currentUser.role}`;
        applyRoleAccess();
        document.getElementById('auth-shell').classList.add('hidden');
        document.getElementById('app-shell').classList.remove('hidden');
        await loadCoreData();
        showSection(currentUser.role === 'Nurse' ? 'nurse-station' : 'dashboard');
        setStatus('Session restored.');
        startAnalyticsPolling();
    } catch {
        logout();
    }
}

document.addEventListener('DOMContentLoaded', () => {
    bindLoginTabs();
    bindLoginForms();
    bindNav();
    bindForms();
    bindDynamicActions();
    document.getElementById('logout-btn').addEventListener('click', logout);
    bootstrapFromToken();
});
