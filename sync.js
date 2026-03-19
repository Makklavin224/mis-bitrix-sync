#!/usr/bin/env node

/**
 * node sync.js 05.03.2026 19.03.2026
 *
 * Импорт ВСЕХ визитов из МИС Реновация → Битрикс24
 * Без лимитов, без таймаутов — работает пока не закончит.
 */

import 'dotenv/config';

const MIS_BASE = 'https://app.rnova.org/api/public';
const BITRIX_URL = process.env.BITRIX_WEBHOOK_URL;
const MIS_API_KEY = process.env.MIS_API_KEY;

if (!BITRIX_URL || !MIS_API_KEY) {
  console.error('Заполните BITRIX_WEBHOOK_URL и MIS_API_KEY в .env');
  process.exit(1);
}

// =====================================================================
//  МИС API
// =====================================================================

async function mis(method, params = {}) {
  const body = new URLSearchParams({ api_key: MIS_API_KEY, ...params });
  const res = await fetch(`${MIS_BASE}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });
  const json = await res.json();
  if (json.error !== 0 || !json.data) return null;
  return Array.isArray(json.data) ? json.data : [json.data];
}

async function getAppointments(dateFrom, dateTo) {
  return (await mis('getAppointments', { date_from: dateFrom, date_to: dateTo, show_patient_data: '1' })) || [];
}

async function checkTreatmentPlan(patientId, visitDate) {
  if (visitDate) {
    const now = new Date();
    const visit = parseDateDMY(visitDate);
    const start = visit > now ? now : visit;
    return await searchPlan(patientId, fmtDate(start), fmtDate(addDays(start, 30)));
  }
  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));
    const r = await searchPlan(patientId, fmtDate(from), fmtDate(to));
    if (r.found) return r;
  }
  return { found: false };
}

async function searchPlan(patientId, dateFrom, dateTo) {
  const data = await mis('getPrograms', { date_from: dateFrom, date_to: dateTo });
  if (!data) return { found: false };
  const m = data.filter(p => String(p.patient_id) === String(patientId));
  if (m.length) return { found: true, title: m[0].title || '', doctor_id: m[0].doctor_id || null };
  return { found: false };
}

async function getTotalPaid(patientId) {
  let total = 0;
  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));
    const data = await mis('getInvoices', { patient_id: patientId, status: '2', date_from: fmtDate(from), date_to: fmtDate(to) });
    if (data) for (const inv of data) total += parseFloat(inv.value) || 0;
  }
  return total;
}

// =====================================================================
//  Шаблоны планов
// =====================================================================

let _tplCache = null, _catCache = null;

async function loadPlanTemplates() {
  if (_tplCache && _catCache) return;
  const cats = await mis('getProgramTemplateCategories');
  _catCache = new Map();
  if (cats) { (function f(items) { for (const i of items) { _catCache.set(String(i.id), i.title.trim()); if (i.children?.length) f(i.children); } })(cats); }
  const tpls = await mis('getProgramTemplates');
  _tplCache = new Map();
  if (tpls) for (const t of tpls) _tplCache.set(t.title, String(t.category_id));
  log(`Шаблоны: ${_catCache.size} категорий, ${_tplCache.size} шаблонов`);
}

function getDirection(title) {
  if (!_tplCache || !_catCache || !title) return null;
  const cid = _tplCache.get(title);
  return cid ? (_catCache.get(cid) || null) : null;
}

const IND_MAP = [
  { kw: ['склеротерапия'], v: 'Склеротерапия' },
  { kw: ['минифлебэктомия'], v: 'Минифлебэктомии' },
  { kw: ['ЭВЛК'], v: 'ЭВЛК' },
  { kw: ['геморроидопластика', 'LHP'], v: 'Лазерная геморроидопластика (LHP)' },
  { kw: ['геморроидэктомия'], v: 'ГеморроидЭктомия' },
  { kw: ['свищ', 'фистул'], v: 'Лечение свищей' },
  { kw: ['склерозирование геморроид'], v: 'Склерозирование геморроидальных узлов' },
  { kw: ['КЛаКС'], v: 'КЛаКС' },
  { kw: ['лазерное омоложение'], v: 'Лазерное омоложение' },
  { kw: ['капельница'], v: 'Капельница' },
  { kw: ['анализ'], v: 'Анализы' },
  { kw: ['операцион'], v: 'Операция' },
  { kw: ['киста'], v: 'Малое инвазивное' },
  { kw: ['тромбированн'], v: 'Малое инвазивное' },
];

function mapPlan(title, dir) {
  const r = {};
  if (dir) r.direction = dir;
  if (title) { const l = title.toLowerCase(); for (const e of IND_MAP) { if (e.kw.some(k => l.includes(k.toLowerCase()))) { r.indication = e.v; break; } } }
  return r;
}

// =====================================================================
//  Битрикс24 API — retry при rate limit
// =====================================================================

async function bx(method, params) {
  const url = `${BITRIX_URL.replace(/\/$/, '')}/${method}`;
  for (let attempt = 0; attempt < 7; attempt++) {
    const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(params) });
    if (!r.ok) throw new Error(`Bitrix ${method}: ${r.status}`);
    const data = await r.json();
    if (data.error === 'QUERY_LIMIT_EXCEEDED') {
      const wait = (attempt + 1) * 2;
      process.stdout.write(`[rate-limit] ${method} — жду ${wait}с... `);
      await sleep(wait * 1000);
      console.log('retry');
      continue;
    }
    if (data.error) throw new Error(`Bitrix ${method}: ${data.error} — ${data.error_description}`);
    return data;
  }
  throw new Error(`Bitrix ${method}: rate limit после 7 попыток`);
}

let _fieldsCache = null;
async function getFields() { if (!_fieldsCache) { _fieldsCache = (await bx('crm.deal.fields', {}))?.result || {}; } return _fieldsCache; }
function findItemId(fieldDef, val) { if (!fieldDef?.items || !val) return null; const m = fieldDef.items.find(i => i.VALUE === val); return m ? m.ID : null; }

async function findDoctorId(name) {
  if (!name) return null;
  const f = await getFields();
  const df = f['UF_CRM_1770366173566'];
  if (!df?.items) return null;
  const parts = name.trim().split(/\s+/);
  let m = null;
  if (parts.length >= 2) { const init = parts[1]?.replace('.', '')[0]; m = df.items.find(i => { const p = i.VALUE.trim().split(/\s+/); return p[0] === parts[0] && p[1]?.[0] === init; }); }
  if (!m) m = df.items.find(i => i.VALUE.startsWith(parts[0]));
  return m ? m.ID : null;
}

async function findDirId(t) { return findItemId((await getFields())['UF_CRM_1770364332145'], t); }
async function findIndId(t) { return findItemId((await getFields())['UF_CRM_1770364702542'], t); }

async function findContact(phone) {
  for (const v of [`+${phone}`, phone, ...(phone.length > 10 ? [phone.slice(-10)] : [])]) {
    const r = await bx('crm.contact.list', { filter: { PHONE: v }, select: ['ID'] });
    if (r?.result?.length) return r.result[0].ID;
  }
  return null;
}

async function createContact(apt, phone) {
  const p = (apt.patient_name || '').trim().split(/\s+/);
  const fields = { NAME: p[1] || '', LAST_NAME: p[0] || '', PHONE: [{ VALUE: `+${phone}`, VALUE_TYPE: 'MOBILE' }] };
  if (p.length > 2) fields.SECOND_NAME = p.slice(2).join(' ');
  if (apt.patient_email) fields.EMAIL = [{ VALUE: apt.patient_email, VALUE_TYPE: 'WORK' }];
  if (apt.patient_birth_date) fields.BIRTHDATE = apt.patient_birth_date;
  return (await bx('crm.contact.add', { fields }))?.result || null;
}

async function findDeals(contactId) {
  return (await bx('crm.deal.list', { filter: { CONTACT_ID: contactId, CLOSED: 'N' }, select: ['ID', 'TITLE', 'STAGE_ID'] }))?.result || [];
}

async function setProducts(dealId, services) {
  if (!services?.length) return;
  const rows = services.filter(s => s.title && s.price).map(s => ({ PRODUCT_NAME: s.title, PRICE: parseFloat(s.price) || 0, QUANTITY: parseInt(s.count) || 1 }));
  if (rows.length) await bx('crm.deal.productrows.set', { id: dealId, rows });
}

async function buildFields(stage, apt, plan, docId, inclPlan) {
  const f = {};
  if (apt.time_start) { f['UF_CRM_1769787005282'] = apt.time_start; if (stage === 'UC_JLJ6EI') f['UF_CRM_1769787080767'] = apt.time_start; }
  if (apt.patient_name) f['UF_CRM_1770364627343'] = apt.patient_name;
  if (apt.patient_birth_date) f['UF_CRM_1772184930463'] = apt.patient_birth_date;
  if (docId) f['UF_CRM_1770366173566'] = docId;
  if (inclPlan && plan?.found) {
    const m = mapPlan(plan.title, plan.direction);
    if (m.direction) { const id = await findDirId(m.direction); if (id) f['UF_CRM_1770364332145'] = id; }
    if (m.indication) { const id = await findIndId(m.indication); if (id) f['UF_CRM_1770364702542'] = id; }
  }
  return f;
}

// =====================================================================
//  Сценарий
// =====================================================================

async function getScenario(apt) {
  const s = apt.status;
  const first = apt.is_first === true || apt.is_first === '1' || apt.is_first === 1 || apt.type === 'Первичный прием';
  let stage, scenario, plan = { found: false };

  if (s === 'upcoming') {
    if (first) { stage = 'FINAL_INVOICE'; scenario = 'new_first_visit'; }
    else { plan = await checkTreatmentPlan(apt.patient_id); stage = plan.found ? 'UC_JLJ6EI' : 'FINAL_INVOICE'; scenario = plan.found ? 'appointment_with_plan' : 'returning_no_plan'; }
  } else if (s === 'completed') {
    const vd = apt.time_start?.split(' ')[0];
    plan = await checkTreatmentPlan(apt.patient_id, vd);
    stage = plan.found ? 'UC_7KB49S' : 'UC_LVYHC1'; scenario = plan.found ? 'completed_with_plan' : 'completed_no_plan';
  } else if (s === 'refused') {
    plan = await checkTreatmentPlan(apt.patient_id);
    stage = plan.found ? 'UC_QAU8BB' : 'UC_1HMFHN'; scenario = plan.found ? 'noshow_with_plan' : 'noshow_no_plan';
  } else return null;

  return { stage, scenario, plan };
}

// =====================================================================
//  Обработка одного визита
// =====================================================================

async function syncOne(apt, allServices) {
  const raw = apt.patient_phone;
  if (!raw) return 'skip:no_phone';
  const digits = raw.replace(/\D/g, '');
  const phone = digits.startsWith('8') ? '7' + digits.slice(1) : digits;
  if (phone.length < 10) return 'skip:short_phone';

  const sc = await getScenario(apt);
  if (!sc) return 'skip:status';

  if (sc.plan.found) { await loadPlanTemplates(); sc.plan.direction = getDirection(sc.plan.title); }

  let cid = await findContact(phone);
  let newContact = false;
  if (!cid) { cid = await createContact(apt, phone); if (!cid) return 'skip:contact_fail'; newContact = true; }

  const docId = await findDoctorId(apt.doctor);
  const proc = (apt.doctor || '').toLowerCase().includes('процедурный кабинет');
  const same = !sc.plan.found || !sc.plan.doctor_id || !apt.doctor_id || String(sc.plan.doctor_id) === String(apt.doctor_id) || proc;

  const fields = await buildFields(sc.stage, apt, sc.plan, docId, sc.plan.found && same);
  const paid = await getTotalPaid(apt.patient_id);
  if (paid > 0) fields['UF_CRM_1770381244477'] = String(paid);

  const deals = await findDeals(cid);

  if (!deals.length) {
    const r = await bx('crm.deal.add', { fields: { STAGE_ID: sc.stage, CONTACT_ID: cid, TITLE: `${apt.patient_name || '?'} — ${sc.scenario}`, ...fields } });
    const did = r?.result;
    if (allServices.length && did) await setProducts(did, allServices);
    return `created:#${did} [${sc.scenario}]${newContact ? ' +contact' : ''}`;
  }

  const actions = [];
  for (const d of deals) {
    if (d.STAGE_ID === sc.stage) {
      if (Object.keys(fields).length) await bx('crm.deal.update', { id: d.ID, fields });
      if (allServices.length) await setProducts(d.ID, allServices);
      actions.push(`#${d.ID} ok`);
    } else {
      await bx('crm.deal.update', { id: d.ID, fields: { STAGE_ID: sc.stage, ...fields } });
      if (allServices.length) await setProducts(d.ID, allServices);
      actions.push(`#${d.ID} ${d.STAGE_ID}→${sc.stage}`);
    }
  }
  return `updated:${actions.join(', ')}`;
}

// =====================================================================
//  Main
// =====================================================================

async function main() {
  const [dateFrom, dateTo] = process.argv.slice(2);
  if (!dateFrom || !dateTo) { console.log('node sync.js ДД.ММ.ГГГГ ДД.ММ.ГГГГ'); process.exit(1); }

  log(`Старт: ${dateFrom} → ${dateTo}`);

  const all = await getAppointments(dateFrom, dateTo);
  log(`Всего из МИС: ${all.length}`);

  all.sort((a, b) => (a.time_start || '').localeCompare(b.time_start || ''));
  const relevant = all.filter(a => a.status === 'upcoming' || a.status === 'completed' || a.status === 'refused');
  log(`Релевантных: ${relevant.length}`);

  // Группируем услуги по пациенту
  const byPatient = new Map();
  for (const a of relevant) {
    if (!a.patient_id) continue;
    if (!byPatient.has(a.patient_id)) byPatient.set(a.patient_id, []);
    if (a.services?.length) byPatient.get(a.patient_id).push(...a.services);
  }

  const stats = { created: 0, updated: 0, skipped: 0, errors: 0, contacts: 0 };
  const startTime = Date.now();

  for (let i = 0; i < relevant.length; i++) {
    const apt = relevant[i];
    const name = apt.patient_name || '?';
    const services = byPatient.get(apt.patient_id) || [];

    try {
      const result = await syncOne(apt, services);
      if (result.startsWith('created')) {
        stats.created++;
        if (result.includes('+contact')) stats.contacts++;
      } else if (result.startsWith('updated')) {
        stats.updated++;
      } else {
        stats.skipped++;
      }
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const speed = ((i + 1) / (elapsed || 1)).toFixed(1);
      log(`[${i + 1}/${relevant.length}] ${name} → ${result}  (${elapsed}с, ${speed}/с)`);
    } catch (err) {
      stats.errors++;
      log(`[${i + 1}/${relevant.length}] ${name} — ОШИБКА: ${err.message}`);
    }
  }

  const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log('');
  log('════════════════════════════════════');
  log(`  Создано сделок:    ${stats.created}`);
  log(`  Создано контактов: ${stats.contacts}`);
  log(`  Обновлено:         ${stats.updated}`);
  log(`  Пропущено:         ${stats.skipped}`);
  log(`  Ошибки:            ${stats.errors}`);
  log(`  Время:             ${totalTime} мин`);
  log('════════════════════════════════════');
}

// Хелперы
function parseDateDMY(s) { const [d, m, y] = s.split('.'); return new Date(+y, +m - 1, +d); }
function fmtDate(d) { return `${String(d.getDate()).padStart(2, '0')}.${String(d.getMonth() + 1).padStart(2, '0')}.${d.getFullYear()}`; }
function addDays(d, n) { const r = new Date(d); r.setDate(r.getDate() + n); return r; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function log(m) { console.log(`[${new Date().toLocaleTimeString('ru-RU')}] ${m}`); }

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
