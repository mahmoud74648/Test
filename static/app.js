const API = '';
let attendanceRows = [];
let latestDate = null;

function showToast(msg, type = 'success') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = `toast ${type} show`;
  setTimeout(() => { t.className = 'toast'; }, 3000);
}

function fmt(v) { return v == null || v === '' ? '—' : v; }

function fmtTime(v) {
  if (v == null || v === '') return '—';
  return String(v).trim().replace(/\s*:\s*/g, ':').replace(/\s*-\s*/g, '-');
}

function fmtRecords(v) {
  if (v == null || v === '') return '—';
  const s = String(v).trim().replace(/\s*:\s*/g, ':').replace(/\s*-\s*/g, '-');
  return s.replace(/[;,|]/g, m => `${m}\u200b`);
}

function parseHmToMinutes(v) {
  if (v == null) return null;
  const s = String(v).trim();
  const m = s.match(/(\d{1,2})\s*:\s*(\d{2})/);
  if (!m) return null;
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
  return hh * 60 + mm;
}

function parseDurationToMinutes(v) {
  if (v == null) return null;
  const s = String(v).trim();
  const m = s.match(/(\d{1,2})\s*:\s*(\d{2})/);
  if (!m) return null;
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  if (hh < 0 || mm < 0 || mm > 59) return null;
  return hh * 60 + mm;
}

function minutesToHm(mins) {
  if (!Number.isFinite(mins)) return null;
  let m = Math.round(mins);
  m = ((m % (24 * 60)) + (24 * 60)) % (24 * 60);
  const hh = Math.floor(m / 60);
  const mm = m % 60;
  return `${String(hh).padStart(2, '0')}:${String(mm).padStart(2, '0')}`;
}

function extractInOut(row) {
  const directIn = row?.actual_check_in_time != null && String(row.actual_check_in_time).trim() !== ''
    ? String(row.actual_check_in_time).trim()
    : null;
  const directOut = row?.actual_check_out_time != null && String(row.actual_check_out_time).trim() !== ''
    ? String(row.actual_check_out_time).trim()
    : null;
  const durationMins = parseDurationToMinutes(row?.total_work_hours);
  if (directIn || directOut) {
    if (durationMins != null) {
      const inM = parseHmToMinutes(directIn);
      const outM = parseHmToMinutes(directOut);
      if (inM != null && outM == null) {
        const computed = minutesToHm(inM + durationMins);
        return { inTime: directIn, outTime: computed };
      }
      if (outM != null && inM == null) {
        const computed = minutesToHm(outM - durationMins);
        return { inTime: computed, outTime: directOut };
      }
    }
    return { inTime: directIn, outTime: directOut };
  }

  const raw = row?.attendance_records;
  if (raw == null || String(raw).trim() === '') return { inTime: null, outTime: null };

  const s = String(raw)
    .replace(/\s*:\s*/g, ':')
    .replace(/\s*-\s*/g, '-')
    .replace(/[؛،]/g, ',');

  const times = (s.match(/\b\d{1,2}:\d{2}\b/g) || [])
    .map(t => t.trim())
    .filter(Boolean);

  if (times.length >= 2) return { inTime: times[0], outTime: times[times.length - 1] };
  if (times.length === 1) {
    if (durationMins != null) {
      const inM = parseHmToMinutes(times[0]);
      if (inM != null) {
        return { inTime: times[0], outTime: minutesToHm(inM + durationMins) };
      }
    }
    return { inTime: times[0], outTime: null };
  }

  const range = s.match(/\b\d{1,2}:\d{2}\b\s*-\s*\b\d{1,2}:\d{2}\b/);
  if (range) {
    const parts = range[0].split('-').map(x => x.trim());
    return { inTime: parts[0] || null, outTime: parts[1] || null };
  }

  return { inTime: null, outTime: null };
}

function deptLabel(rawDept) {
  if (rawDept == null || rawDept === '') return '—';
  const s = String(rawDept);
  const re = /all\s*departments\s*>\s*القوة\s*>/i;
  if (re.test(s)) return s.replace(re, '').trim() || s;
  const m = s.match(/القوة\s*>/);
  if (m && m.index != null) return s.slice(m.index + m[0].length).trim() || s;
  return s;
}

function getParam(name) {
  const v = new URLSearchParams(window.location.search).get(name);
  return v == null || v === '' ? null : v;
}

function getSelectedDate() {
  const v = document.getElementById('dateFilter')?.value;
  return v && v.length >= 10 ? v : null;
}

window.addEventListener('DOMContentLoaded', () => {
  loadStats();
  initDateAndLoad();
});

async function initDateAndLoad() {
  const dateInput = document.getElementById('dateFilter');
  const fromQuery = getParam('date');
  if (dateInput && fromQuery) dateInput.value = fromQuery;

  if (!getSelectedDate()) {
    try {
      const r = await fetch(`${API}/latest-date`);
      const d = await r.json();
      latestDate = d.date || null;
      if (dateInput && latestDate) dateInput.value = latestDate;
    } catch { /* silent */ }
  }

  if (dateInput) dateInput.onchange = () => reloadForDate();
  await reloadForDate();
}

async function loadStats() {
  try {
    const r = await fetch(`${API}/stats`);
    const d = await r.json();
    const totalEl = document.getElementById('stat-total');
    if (totalEl) totalEl.textContent = d.total_employees ?? 0;

    const activeEl = document.getElementById('stat-active');
    if (activeEl) activeEl.textContent = d.active_employees ?? 0;

    const avgEl = document.getElementById('stat-avgleave');
    if (avgEl) avgEl.textContent = d.avg_leave_days ?? 0;

    const deptsEl = document.getElementById('stat-depts');
    if (deptsEl) deptsEl.textContent = d.department_count ?? 0;
  } catch { /* silent */ }
}

async function reloadForDate() {
  const d = getSelectedDate();
  const dateText = document.getElementById('selectedDateText');
  if (dateText) dateText.textContent = d || latestDate || '—';
  await loadAttendance(d);
  filterTable();
}

async function loadAttendance(date) {
  try {
    const url = date ? `${API}/attendance?date=${encodeURIComponent(date)}` : `${API}/attendance`;
    const r = await fetch(url);
    attendanceRows = await r.json();
  } catch {
    attendanceRows = [];
    document.getElementById('empBody').innerHTML =
      '<tr><td colspan="6" class="loading-row">تعذر تحميل بيانات الحضور.</td></tr>';
  }
}

function renderTable(rows) {
  const tbody = document.getElementById('empBody');
  document.getElementById('rowCount').textContent = rows.length;

  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="loading-row">لا توجد نتائج مطابقة.</td></tr>';
    return;
  }

  tbody.innerHTML = rows.map(e => `
    <tr>
      <td class="col-id">${fmt(e.employee_code)}</td>
      <td class="col-name"><strong>${fmt(e.name)}</strong></td>
      <td class="col-dept">${deptLabel(e.dept)}</td>
      <td class="col-date"><span dir="ltr">${fmt(e.date)}</span></td>
      <td class="col-io"><span dir="ltr">${(() => {
        const io = extractInOut(e);
        const a = fmtTime(io.inTime);
        const b = fmtTime(io.outTime);
        if (a === '—' && b === '—') return '—';
        if (a !== '—' && b !== '—') return `${a} - ${b}`;
        return a !== '—' ? a : b;
      })()}</span></td>
      <td class="col-work"><span dir="ltr">${fmtTime(e.total_work_hours)}</span></td>
    </tr>`).join('');
}

function filterTable() {
  const q = document.getElementById('searchInput').value.toLowerCase();
  const onlyForce = document.getElementById('forceFilter')?.checked ?? false;

  const filtered = attendanceRows.filter(e => {
    const code = String(e.employee_code ?? '').toLowerCase();
    const name = String(e.name ?? '').toLowerCase();
    const matchQ = !q || code.includes(q) || name.includes(q);
    const matchForce = !onlyForce || String(e.dept ?? '').includes('القوة');
    return matchQ && matchForce;
  });
  renderTable(filtered);
}

async function importExcel() {
  try {
    const r = await fetch(`${API}/import/excel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file: 'test0-hik.xlsx', reset_db: false }),
    });
    if (!r.ok) throw new Error();
    const res = await r.json();
    showToast(`تم الاستيراد بنجاح: ${res.attendance_rows_upserted} سجل من ${res.file}`);
    await loadStats();
    await reloadForDate();
  } catch {
    showToast('فشل الاستيراد. تأكد من وجود ملف test0-hik.xlsx', 'error');
  }
}
