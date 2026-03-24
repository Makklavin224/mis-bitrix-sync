#!/usr/bin/env node
import 'dotenv/config';

/**
 * Полная сверка сделок CRM с данными МИС.
 *
 * Для каждой активной сделки проверяет:
 * 1. Сумма оплат в CRM = сумма оплаченных счетов в МИС?
 * 2. Дата записи в CRM = ближайшая запись в МИС?
 * 3. Врач заполнен?
 * 4. ФИО пациента совпадает?
 *
 * node verify-deals.js                        — отчёт
 * DRY_RUN=0 node verify-deals.js              — исправить расхождения
 * STAGE=UC_7KB49S node verify-deals.js        — только конкретная стадия
 */

const MIS_BASE = 'https://app.rnova.org/api/public';
const CLOSED_STAGES = ['UC_NCW0DT', 'UC_F92MOY', 'WON', 'LOSE', 'APOLOGY'];

const BITRIX_URL = process.env.BITRIX_WEBHOOK_URL;
const MIS_API_KEY = process.env.MIS_API_KEY;
const DRY_RUN = process.env.DRY_RUN !== '0';
const FILTER_STAGE = process.env.STAGE || null;

if (!BITRIX_URL || !MIS_API_KEY) {
  console.error('Нужны переменные: BITRIX_WEBHOOK_URL и MIS_API_KEY');
  process.exit(1);
}

const STAGE_NAMES = {
  'FINAL_INVOICE': 'Записан на консультацию',
  'UC_JLJ6EI': 'Записан на лечение',
  'UC_7KB49S': 'Есть показания к лечению',
  'UC_LVYHC1': 'Нет показаний к лечению',
  'UC_QAU8BB': 'Не пришёл на лечение',
  'UC_1HMFHN': 'Не пришёл на консультацию',
};

console.log(`\n${'='.repeat(55)}`);
console.log(`  Сверка сделок CRM ↔ МИС`);
console.log(`  Режим: ${DRY_RUN ? 'ОТЧЁТ' : 'ИСПРАВЛЕНИЕ'}`);
if (FILTER_STAGE) console.log(`  Стадия: ${STAGE_NAMES[FILTER_STAGE] || FILTER_STAGE}`);
console.log(`${'='.repeat(55)}\n`);

// ===== API =====

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function bitrix(method, params, attempt = 1) {
  const url = `${BITRIX_URL.replace(/\/$/, '')}/${method}`;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
    });
    const data = await res.json();
    if (data.error === 'QUERY_LIMIT_EXCEEDED' && attempt <= 5) {
      await sleep(1000 * attempt);
      return bitrix(method, params, attempt + 1);
    }
    if (data.error) throw new Error(`Bitrix ${method}: ${data.error} — ${data.error_description}`);
    return data;
  } catch (err) {
    if (attempt <= 5) {
      await sleep(1000 * attempt);
      return bitrix(method, params, attempt + 1);
    }
    throw err;
  }
}

async function misPost(method, params) {
  const urlParams = new URLSearchParams({ api_key: MIS_API_KEY, ...params });
  const res = await fetch(`${MIS_BASE}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: urlParams.toString(),
  });
  return await res.json();
}

function fmtDate(d) {
  return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()}`;
}
function addDays(d, n) { const r = new Date(d); r.setDate(r.getDate() + n); return r; }

// ===== Загрузка =====

async function getAllDeals() {
  let all = [];
  let start = 0;
  const filter = { CLOSED: 'N' };
  if (FILTER_STAGE) filter.STAGE_ID = FILTER_STAGE;

  while (true) {
    const res = await bitrix('crm.deal.list', {
      filter,
      select: [
        'ID', 'TITLE', 'STAGE_ID', 'CONTACT_ID', 'OPPORTUNITY',
        'UF_CRM_1774345475',       // врач
        'UF_CRM_1770381244477',    // сумма оплат
        'UF_CRM_1769787005282',    // дата записи
        'UF_CRM_1769787080767',    // дата записи на лечение
        'UF_CRM_1770364627343',    // ФИО пациента
      ],
      start,
    });
    const deals = res.result || [];
    all.push(...deals);
    process.stdout.write(`\r  Загружено сделок: ${all.length}...`);
    if (!res.next) break;
    start = res.next;
  }
  console.log(`\r  Всего: ${all.length}                    `);
  return all;
}

// Кэши
const phoneCache = new Map();
const patientCache = new Map();

async function getContactPhone(contactId) {
  if (phoneCache.has(contactId)) return phoneCache.get(contactId);
  const res = await bitrix('crm.contact.get', { id: contactId });
  const phone = res?.result?.PHONE?.[0]?.VALUE || null;
  phoneCache.set(contactId, phone);
  return phone;
}

async function findPatient(phone) {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, '');
  const normalized = digits.startsWith('8') ? '7' + digits.slice(1) : digits;
  if (patientCache.has(normalized)) return patientCache.get(normalized);

  for (const variant of [normalized, normalized.slice(-10)]) {
    const json = await misPost('getPatient', { mobile: variant });
    if (json.error === 0 && json.data) {
      const patients = Array.isArray(json.data) ? json.data : [json.data];
      if (patients.length) {
        patientCache.set(normalized, patients[0]);
        return patients[0];
      }
    }
  }
  patientCache.set(normalized, null);
  return null;
}

async function getTotalPaid(patientId) {
  const now = new Date();
  let total = 0;
  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));
    const json = await misPost('getInvoices', {
      patient_id: patientId,
      status: '2',
      date_from: fmtDate(from),
      date_to: fmtDate(to),
    });
    if (json.error === 0 && json.data) {
      const invoices = Array.isArray(json.data) ? json.data : [json.data];
      for (const inv of invoices) total += parseFloat(inv.value) || 0;
    }
  }
  return total;
}

async function getNextAppointment(patientId) {
  const now = new Date();
  const json = await misPost('getAppointments', {
    patient_id: patientId,
    status: 'upcoming',
    date_from: `${fmtDate(now)} 00:00`,
    date_to: `${fmtDate(addDays(now, 60))} 23:59`,
  });
  if (json.error !== 0 || !json.data) return null;
  const appts = Array.isArray(json.data) ? json.data : [json.data];
  appts.sort((a, b) => (a.time_start || '').localeCompare(b.time_start || ''));
  return appts[0] || null;
}

// ===== Основная логика =====

async function main() {
  console.log('[1/2] Загрузка сделок...');
  const deals = await getAllDeals();
  const activeDeals = deals.filter(d => !CLOSED_STAGES.includes(d.STAGE_ID));
  console.log(`  Активных: ${activeDeals.length}\n`);

  console.log('[2/2] Сверка с МИС...\n');

  const issues = [];
  let checked = 0;
  let skipped = 0;

  for (const deal of activeDeals) {
    checked++;
    if (checked % 10 === 0) {
      process.stdout.write(`\r  Проверено: ${checked}/${activeDeals.length}...`);
    }

    if (!deal.CONTACT_ID) { skipped++; continue; }

    const phone = await getContactPhone(deal.CONTACT_ID);
    if (!phone) { skipped++; continue; }

    const patient = await findPatient(phone);
    if (!patient) { skipped++; continue; }

    const patientId = String(patient.patient_id);
    const patientName = `${patient.last_name || ''} ${patient.first_name || ''} ${patient.third_name || ''}`.trim();
    const dealProblems = [];
    const fixes = {};

    // 1. Сумма оплат
    const misPaid = await getTotalPaid(patientId);
    const crmPaid = parseFloat(deal.UF_CRM_1770381244477) || 0;
    if (Math.abs(misPaid - crmPaid) > 1) {
      dealProblems.push({
        field: 'Сумма оплат',
        crm: `${crmPaid.toLocaleString('ru-RU')} ₽`,
        mis: `${misPaid.toLocaleString('ru-RU')} ₽`,
        diff: `${(misPaid - crmPaid).toLocaleString('ru-RU')} ₽`,
      });
      fixes['UF_CRM_1770381244477'] = String(misPaid);
    }

    // 2. Ближайшая запись
    const nextAppt = await getNextAppointment(patientId);
    const crmDate = deal.UF_CRM_1769787005282 || '';
    const misDate = nextAppt?.time_start || '';

    if (nextAppt && crmDate !== misDate) {
      dealProblems.push({
        field: 'Дата записи',
        crm: crmDate || '(пусто)',
        mis: misDate,
      });
      fixes['UF_CRM_1769787005282'] = misDate;
    }

    if (!nextAppt && crmDate) {
      dealProblems.push({
        field: 'Дата записи',
        crm: crmDate,
        mis: '(нет записей)',
      });
      // Не очищаем — возможно запись дальше 60 дней
    }

    // 3. Врач
    if (!deal.UF_CRM_1774345475) {
      dealProblems.push({
        field: 'Врач',
        crm: '(пусто)',
        mis: nextAppt?.doctor || '—',
      });
    }

    // 4. ФИО пациента
    const crmName = (deal.UF_CRM_1770364627343 || '').trim();
    if (patientName && crmName && patientName !== crmName) {
      dealProblems.push({
        field: 'ФИО',
        crm: crmName,
        mis: patientName,
      });
      fixes['UF_CRM_1770364627343'] = patientName;
    }
    if (!crmName && patientName) {
      dealProblems.push({
        field: 'ФИО',
        crm: '(пусто)',
        mis: patientName,
      });
      fixes['UF_CRM_1770364627343'] = patientName;
    }

    if (dealProblems.length) {
      issues.push({
        dealId: deal.ID,
        title: deal.TITLE,
        stage: deal.STAGE_ID,
        doctor: deal.UF_CRM_1774345475 || '?',
        problems: dealProblems,
        fixes,
      });
    }
  }

  console.log(`\r  Проверено: ${checked}/${activeDeals.length}      \n`);

  // Отчёт
  if (!issues.length) {
    console.log('  Все данные совпадают!\n');
    return;
  }

  // Группировка по типу проблемы
  const bySummary = {};
  for (const issue of issues) {
    for (const p of issue.problems) {
      if (!bySummary[p.field]) bySummary[p.field] = { count: 0, deals: [] };
      bySummary[p.field].count++;
      bySummary[p.field].deals.push(issue);
    }
  }

  console.log('  Расхождения по типам:\n');
  for (const [field, data] of Object.entries(bySummary)) {
    console.log(`  ${field}: ${data.count} сделок`);
  }
  console.log('');

  // Детали — первые 15
  console.log('  Детали (первые 15):\n');
  for (const issue of issues.slice(0, 15)) {
    console.log(`  #${issue.dealId} "${issue.title}" | ${issue.doctor} [${STAGE_NAMES[issue.stage] || issue.stage}]`);
    for (const p of issue.problems) {
      const diffStr = p.diff ? ` (разница: ${p.diff})` : '';
      console.log(`    ${p.field}: CRM="${p.crm}" → МИС="${p.mis}"${diffStr}`);
    }
  }
  if (issues.length > 15) console.log(`\n  ... и ещё ${issues.length - 15} сделок`);

  // Исправление
  if (!DRY_RUN) {
    const fixable = issues.filter(i => Object.keys(i.fixes).length > 0);
    console.log(`\n  Исправляю ${fixable.length} сделок...`);
    for (const issue of fixable) {
      await bitrix('crm.deal.update', {
        id: issue.dealId,
        fields: issue.fixes,
      });
    }
    console.log('  Готово.');
  }

  console.log(`\n${'='.repeat(55)}`);
  console.log(`  ИТОГ`);
  console.log(`${'='.repeat(55)}`);
  console.log(`  Проверено:       ${checked}`);
  console.log(`  Пропущено:       ${skipped}`);
  console.log(`  С расхождениями: ${issues.length}`);
  const fixable = issues.filter(i => Object.keys(i.fixes).length > 0);
  console.log(`  Исправимых:      ${fixable.length}`);
  if (DRY_RUN && fixable.length) {
    console.log(`\n  Для исправления: DRY_RUN=0 node verify-deals.js`);
  }
  console.log('');
}

main().catch(err => {
  console.error('\nОшибка:', err.message);
  process.exit(1);
});
