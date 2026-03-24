#!/usr/bin/env node
import 'dotenv/config';

/**
 * Аудит стадий сделок: проверяет что сделки стоят на правильных стадиях.
 *
 * Для каждой активной сделки:
 * 1. Находит контакт → телефон → пациента в МИС
 * 2. Проверяет: есть ли план лечения? есть ли будущие записи?
 * 3. Сравнивает текущую стадию с правильной
 * 4. Выводит отчёт и (опционально) исправляет
 *
 * Запуск:
 *   node audit-stages.js                      — только отчёт
 *   DRY_RUN=0 node audit-stages.js            — исправить стадии
 *   STAGE=UC_LVYHC1 node audit-stages.js      — проверить только конкретную стадию
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

// Названия стадий для вывода
const STAGE_NAMES = {
  'FINAL_INVOICE': 'Записан на консультацию',
  'UC_JLJ6EI': 'Записан на лечение',
  'UC_7KB49S': 'Есть показания к лечению',
  'UC_LVYHC1': 'Нет показаний к лечению',
  'UC_QAU8BB': 'Не пришёл на лечение',
  'UC_1HMFHN': 'Не пришёл на консультацию',
  'UC_F92MOY': 'Лечение завершено',
  'UC_NCW0DT': 'Некачественная',
  'WON': 'Успех',
  'LOSE': 'Провал',
  'APOLOGY': 'Анализ провала',
};

function stageName(id) {
  return STAGE_NAMES[id] || id;
}

console.log(`\n${'='.repeat(55)}`);
console.log(`  Аудит стадий сделок`);
console.log(`  Режим: ${DRY_RUN ? 'ОТЧЁТ (только показать)' : 'ИСПРАВЛЕНИЕ (будет менять стадии)'}`);
if (FILTER_STAGE) console.log(`  Фильтр: только стадия ${stageName(FILTER_STAGE)} (${FILTER_STAGE})`);
console.log(`${'='.repeat(55)}\n`);

// ===== API =====

let bitrixCalls = 0;

async function bitrix(method, params, attempt = 1) {
  const url = `${BITRIX_URL.replace(/\/$/, '')}/${method}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  const data = await res.json();
  bitrixCalls++;

  if (data.error === 'QUERY_LIMIT_EXCEEDED' && attempt <= 3) {
    console.log(`\n  [rate limit] пауза 2с... (попытка ${attempt}/3)`);
    await sleep(2000);
    return bitrix(method, params, attempt + 1);
  }

  if (data.error) throw new Error(`Bitrix ${method}: ${data.error} — ${data.error_description}`);
  return data;
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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function fmtDate(d) {
  return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()}`;
}
function addDays(d, n) { const r = new Date(d); r.setDate(r.getDate() + n); return r; }

// Товары-триггеры завершения лечения
const TREATMENT_COMPLETE_KEYWORDS = [
  'повторный прием 2-й день',
  'повторный прием на 2-ой день',
  'повторный прием 7 день',
  'повторный прием на 7-ой день',
  'повторный прием 14 день',
  'повторный прием на 21-й день',
  'повторный прием на 45-й день',
  'контрольный осмотр после лечения',
  'осмотр врача м',
];

/** Проверить товары сделки на наличие контрольных приёмов */
async function checkDealProducts(dealId) {
  const res = await bitrix('crm.deal.productrows.get', { id: dealId });
  const products = res?.result || [];

  for (const p of products) {
    const title = (p.PRODUCT_NAME || '').toLowerCase();
    if (TREATMENT_COMPLETE_KEYWORDS.some(kw => title.includes(kw))) {
      return { found: true, title: p.PRODUCT_NAME };
    }
  }
  return { found: false };
}

// ===== Загрузка данных =====

async function getAllDeals() {
  let all = [];
  let start = 0;
  const filter = { CLOSED: 'N' };
  if (FILTER_STAGE) filter.STAGE_ID = FILTER_STAGE;

  while (true) {
    const res = await bitrix('crm.deal.list', {
      filter,
      select: ['ID', 'TITLE', 'STAGE_ID', 'CONTACT_ID', 'OPPORTUNITY', 'UF_CRM_1774345475'],
      start,
    });
    const deals = res.result || [];
    all.push(...deals);
    process.stdout.write(`\r  Загружено сделок: ${all.length}...`);
    if (!res.next) break;
    start = res.next;
    await sleep(200);
  }
  console.log(`\r  Всего сделок для проверки: ${all.length}   `);
  return all;
}

// Кэш: contactId → phone
const phoneCache = new Map();

async function getContactPhone(contactId) {
  if (phoneCache.has(contactId)) return phoneCache.get(contactId);

  const res = await bitrix('crm.contact.get', { id: contactId });
  const contact = res?.result;
  let phone = null;

  if (contact?.PHONE?.length) {
    phone = contact.PHONE[0].VALUE;
  }

  phoneCache.set(contactId, phone);
  return phone;
}

// Кэш: phone → patientId
const patientCache = new Map();

async function findPatientByPhone(phone) {
  if (!phone) return null;

  const digits = phone.replace(/\D/g, '');
  const normalized = digits.startsWith('8') ? '7' + digits.slice(1) : digits;

  if (patientCache.has(normalized)) return patientCache.get(normalized);

  // Пробуем разные форматы
  const variants = [normalized, normalized.slice(-10)];

  for (const variant of variants) {
    const json = await misPost('getPatient', { mobile: variant });
    if (json.error === 0 && json.data) {
      const patients = Array.isArray(json.data) ? json.data : [json.data];
      if (patients.length) {
        const pid = String(patients[0].patient_id);
        patientCache.set(normalized, pid);
        return pid;
      }
    }
  }

  patientCache.set(normalized, null);
  return null;
}

// Кэш: patientId → plan
const planCache = new Map();

async function checkPlan(patientId) {
  if (planCache.has(patientId)) return planCache.get(patientId);

  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));

    const json = await misPost('getPrograms', {
      date_from: fmtDate(from),
      date_to: fmtDate(to),
    });

    if (json.error === 0 && json.data) {
      const programs = Array.isArray(json.data) ? json.data : [json.data];
      const match = programs.filter(p => String(p.patient_id) === String(patientId));
      if (match.length) {
        const result = { found: true, title: match[0].title };
        planCache.set(patientId, result);
        return result;
      }
    }
  }

  const result = { found: false };
  planCache.set(patientId, result);
  return result;
}

async function getUpcomingAppointments(patientId) {
  const now = new Date();
  const json = await misPost('getAppointments', {
    patient_id: patientId,
    status: 'upcoming',
    date_from: `${fmtDate(now)} 00:00`,
    date_to: `${fmtDate(addDays(now, 60))} 23:59`,
  });

  if (json.error !== 0 || !json.data) return [];
  const appts = Array.isArray(json.data) ? json.data : [json.data];
  return appts;
}

// ===== Определение правильной стадии =====

function determineCorrectStage(currentStage, hasPlan, hasUpcoming, hasTreatmentCompleteProduct) {
  // Терминальные — не трогаем
  if (CLOSED_STAGES.includes(currentStage)) return currentStage;

  // Товар контрольного/повторного приёма → лечение завершено
  if (hasTreatmentCompleteProduct) return 'UC_F92MOY';

  // "Не пришёл" стадии — если есть будущая запись, значит перезаписался
  if (currentStage === 'UC_QAU8BB' || currentStage === 'UC_1HMFHN') {
    if (hasUpcoming && hasPlan) return 'UC_JLJ6EI';    // записан на лечение
    if (hasUpcoming && !hasPlan) return 'FINAL_INVOICE'; // записан на консультацию
    return currentStage; // действительно не пришёл
  }

  // "Нет показаний к лечению" — но если есть план, то есть показания
  if (currentStage === 'UC_LVYHC1') {
    if (hasPlan && hasUpcoming) return 'UC_JLJ6EI';  // записан на лечение
    if (hasPlan) return 'UC_7KB49S';                  // есть показания
    if (hasUpcoming) return 'FINAL_INVOICE';          // записан на консультацию
    return currentStage;
  }

  // "Есть показания к лечению" — если записан, то на лечение
  if (currentStage === 'UC_7KB49S') {
    if (hasUpcoming) return 'UC_JLJ6EI'; // записан на лечение
    return currentStage;
  }

  return currentStage;
}

// ===== Основная логика =====

async function main() {
  console.log('[1/3] Загрузка сделок из Битрикса...');
  const deals = await getAllDeals();

  // Фильтруем терминальные
  const activeDeals = deals.filter(d => !CLOSED_STAGES.includes(d.STAGE_ID));
  console.log(`  Активных (не терминальных): ${activeDeals.length}\n`);

  console.log('[2/3] Проверка каждой сделки через МИС...\n');

  const issues = [];
  let checked = 0;
  let noContact = 0;
  let noPhone = 0;
  let noPatient = 0;

  for (const deal of activeDeals) {
    checked++;
    if (checked % 10 === 0) {
      process.stdout.write(`\r  Проверено: ${checked}/${activeDeals.length}...`);
    }

    if (!deal.CONTACT_ID) {
      noContact++;
      continue;
    }

    // Контакт → телефон
    const phone = await getContactPhone(deal.CONTACT_ID);
    if (!phone) {
      noPhone++;
      continue;
    }
    await sleep(500);

    // Телефон → пациент МИС
    const patientId = await findPatientByPhone(phone);
    if (!patientId) {
      noPatient++;
      continue;
    }
    await sleep(300);

    // Проверить товары на контрольные приёмы
    const products = await checkDealProducts(deal.ID);
    await sleep(500);

    // Проверить план и записи
    const plan = await checkPlan(patientId);
    await sleep(300);

    const upcoming = await getUpcomingAppointments(patientId);
    await sleep(100);

    const correctStage = determineCorrectStage(deal.STAGE_ID, plan.found, upcoming.length > 0, products.found);

    if (correctStage !== deal.STAGE_ID) {
      const amount = parseFloat(deal.OPPORTUNITY) || 0;
      issues.push({
        dealId: deal.ID,
        title: deal.TITLE,
        doctor: deal.UF_CRM_1774345475 || '?',
        amount,
        currentStage: deal.STAGE_ID,
        correctStage,
        hasPlan: plan.found,
        planTitle: plan.title || '',
        hasCompleteProduct: products.found,
        completeProductTitle: products.title || '',
        upcomingCount: upcoming.length,
        nextDate: upcoming[0]?.time_start || '',
      });
    }
  }

  console.log(`\r  Проверено: ${checked}/${activeDeals.length}      \n`);

  // 3. Отчёт
  console.log('[3/3] Отчёт\n');

  if (!issues.length) {
    console.log('  Все сделки на правильных стадиях!\n');
  } else {
    // Группируем по типу ошибки
    const byType = new Map();
    for (const issue of issues) {
      const key = `${issue.currentStage} → ${issue.correctStage}`;
      if (!byType.has(key)) byType.set(key, []);
      byType.get(key).push(issue);
    }

    let totalAmount = 0;

    for (const [type, typeIssues] of byType) {
      const [from, to] = type.split(' → ');
      const sum = typeIssues.reduce((s, i) => s + i.amount, 0);
      totalAmount += sum;

      console.log(`  ${stageName(from)} → ${stageName(to)}`);
      console.log(`  Сделок: ${typeIssues.length}, сумма: ${sum.toLocaleString('ru-RU')} ₽\n`);

      for (const issue of typeIssues.slice(0, 10)) {
        const planInfo = issue.hasPlan ? `план: "${issue.planTitle}"` : 'нет плана';
        const upInfo = issue.upcomingCount ? `записей: ${issue.upcomingCount}, ближ: ${issue.nextDate}` : 'нет записей';
        const prodInfo = issue.hasCompleteProduct ? `товар: "${issue.completeProductTitle}"` : '';
        console.log(`    #${issue.dealId} "${issue.title}" | ${issue.doctor} | ${issue.amount.toLocaleString('ru-RU')} ₽`);
        console.log(`      ${planInfo} | ${upInfo}${prodInfo ? ' | ' + prodInfo : ''}`);
      }
      if (typeIssues.length > 10) {
        console.log(`    ... и ещё ${typeIssues.length - 10} сделок`);
      }
      console.log('');

      // Исправление
      if (!DRY_RUN) {
        console.log(`    Исправляю ${typeIssues.length} сделок...`);
        for (const issue of typeIssues) {
          await bitrix('crm.deal.update', {
            id: issue.dealId,
            fields: { STAGE_ID: issue.correctStage },
          });
          await sleep(300);
        }
        console.log(`    Готово.\n`);
      }
    }

    console.log(`${'='.repeat(55)}`);
    console.log(`  ИТОГ`);
    console.log(`${'='.repeat(55)}`);
    console.log(`  Проверено сделок:    ${checked}`);
    console.log(`  Без контакта:        ${noContact}`);
    console.log(`  Без телефона:        ${noPhone}`);
    console.log(`  Не найден в МИС:    ${noPatient}`);
    console.log(`  На неверной стадии:  ${issues.length}`);
    console.log(`  Сумма неверных:      ${totalAmount.toLocaleString('ru-RU')} ₽`);
    console.log(`  API Битрикс:         ${bitrixCalls}`);

    if (DRY_RUN) {
      console.log(`\n  Режим ОТЧЁТ — ничего не изменено`);
      console.log(`  Для исправления: DRY_RUN=0 node audit-stages.js`);
      if (!FILTER_STAGE) {
        console.log(`  Проверить одну стадию: STAGE=UC_LVYHC1 node audit-stages.js`);
      }
    } else {
      console.log(`\n  Исправлено: ${issues.length} сделок`);
    }
  }

  console.log('');
}

main().catch(err => {
  console.error('\nОшибка:', err.message);
  process.exit(1);
});
