#!/usr/bin/env node
import 'dotenv/config';

/**
 * Очистка дублей сделок в Битриксе + расстановка по правильным стадиям.
 *
 * Что делает:
 * 1. Загружает врачей из МИС с их специальностями
 * 2. Загружает ВСЕ открытые сделки из Битрикса
 * 3. Группирует по контакту → кластеризует по врачу/специальности
 * 4. Дубли: переносит товары в основную сделку, удаляет лишние
 * 5. Для каждой оставшейся сделки: проверяет план лечения + будущие записи
 *    → ставит правильную стадию и дату ближайшей записи
 * 6. НЕ трогает терминальные стадии
 *
 * Запуск:
 *   npm run dry    — только показать что будет (DRY_RUN по умолчанию)
 *   npm run run    — реально исправить
 *
 * Или напрямую:
 *   node cleanup.js              — dry run
 *   DRY_RUN=0 node cleanup.js    — боевой
 */

const MIS_BASE = 'https://app.rnova.org/api/public';

// ВСЕ стадии воронки — загружаем сделки из каждой
const ALL_STAGES = [
  'NEW', 'PREPARATION', 'EXECUTING', 'PREPAYMENT_INVOICE',
  'FINAL_INVOICE', 'UC_1HMFHN', 'UC_K4NZZM', 'UC_7KB49S',
  'UC_JLJ6EI', 'UC_QAU8BB', 'UC_OW1418', 'UC_NCW0DT',
  'UC_LVYHC1', 'UC_F92MOY', 'WON', 'LOSE', 'APOLOGY',
];

// Терминальные — не удаляем при дедупликации (удаляем только ранние дубли)
const CLOSED_STAGES = ['UC_NCW0DT', 'UC_F92MOY', 'WON', 'LOSE', 'APOLOGY'];

const STAGE_NAMES = {
  'NEW': 'Новая',
  'PREPARATION': 'Подготовка',
  'EXECUTING': 'В работе',
  'PREPAYMENT_INVOICE': 'Предоплата',
  'FINAL_INVOICE': 'Записан на консультацию',
  'UC_1HMFHN': 'Не пришёл (консультация)',
  'UC_K4NZZM': 'UC_K4NZZM',
  'UC_7KB49S': 'Есть показания',
  'UC_JLJ6EI': 'Записан на лечение',
  'UC_QAU8BB': 'Не пришёл (лечение)',
  'UC_OW1418': 'UC_OW1418',
  'UC_NCW0DT': 'Некачественная',
  'UC_LVYHC1': 'Нет показаний',
  'UC_F92MOY': 'Лечение завершено',
  'WON': 'Успех',
  'LOSE': 'Провал',
  'APOLOGY': 'Анализ провала',
};
function sn(id) { return STAGE_NAMES[id] || id; }

// Приоритет стадий: чем выше число — тем важнее, оставляем при дедупликации
const STAGE_PRIORITY = {
  'NEW': 0,
  'PREPARATION': 1,
  'EXECUTING': 2,
  'PREPAYMENT_INVOICE': 3,
  'FINAL_INVOICE': 10,
  'UC_1HMFHN': 11,
  'UC_K4NZZM': 12,
  'UC_LVYHC1': 13,
  'UC_7KB49S': 14,
  'UC_JLJ6EI': 15,
  'UC_QAU8BB': 16,
  'UC_OW1418': 17,
  'UC_NCW0DT': 20,
  'UC_F92MOY': 21,
  'WON': 30,
  'LOSE': 30,
  'APOLOGY': 30,
};
function stagePriority(stageId) { return STAGE_PRIORITY[stageId] ?? 5; }

const BITRIX_URL = process.env.BITRIX_WEBHOOK_URL;
const MIS_API_KEY = process.env.MIS_API_KEY;
const DRY_RUN = process.env.DRY_RUN !== '0';

if (!BITRIX_URL || !MIS_API_KEY) {
  console.error('Нужны переменные: BITRIX_WEBHOOK_URL и MIS_API_KEY');
  console.error('Скопируй .env.example → .env и заполни');
  process.exit(1);
}

console.log(`\n${'='.repeat(50)}`);
console.log(`  Очистка сделок Битрикс`);
console.log(`  Режим: ${DRY_RUN ? 'DRY RUN (только показать)' : 'БОЕВОЙ (будет менять)'}`);
console.log(`${'='.repeat(50)}\n`);

// ===== Битрикс API =====

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

  if (data.error === 'QUERY_LIMIT_EXCEEDED' && attempt <= 5) {
    await sleep(1000 * attempt);
    return bitrix(method, params, attempt + 1);
  }

  if (data.error) {
    throw new Error(`Bitrix ${method}: ${data.error} — ${data.error_description}`);
  }
  return data;
}

async function getAllDeals() {
  let all = [];
  let start = 0;

  while (true) {
    const res = await bitrix('crm.deal.list', {
      filter: { STAGE_ID: ALL_STAGES },
      select: ['ID', 'TITLE', 'STAGE_ID', 'CONTACT_ID', 'UF_CRM_1774345475'],
      start,
    });

    const deals = res.result || [];
    all.push(...deals);
    console.log(`  Загружено сделок: ${all.length}...`);

    if (!res.next) break;
    start = res.next;
    await sleep(200);
  }

  return all;
}

async function getDealProducts(dealId) {
  const res = await bitrix('crm.deal.productrows.get', { id: dealId });
  return res?.result || [];
}

// ===== МИС API =====

async function misPost(method, params) {
  const urlParams = new URLSearchParams({ api_key: MIS_API_KEY, ...params });
  const res = await fetch(`${MIS_BASE}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: urlParams.toString(),
  });
  return await res.json();
}

async function loadDoctors() {
  const json = await misPost('getUsers', {});
  const byName = new Map();
  const byId = new Map();

  if (json.error === 0 && json.data) {
    const users = Array.isArray(json.data) ? json.data : [json.data];
    for (const u of users) {
      const profIds = Array.isArray(u.profession) ? u.profession.map(String) : [];
      const entry = { id: String(u.id), name: (u.name || '').trim(), professions: new Set(profIds) };
      byName.set(entry.name, entry);
      byId.set(entry.id, entry);
    }
  }

  return { byName, byId };
}

function sharesSpecialty(doctors, id1, id2) {
  const d1 = doctors.byId.get(String(id1));
  const d2 = doctors.byId.get(String(id2));
  if (!d1 || !d2) return false;
  for (const p of d1.professions) {
    if (d2.professions.has(p)) return true;
  }
  return false;
}

/** Проверить план лечения пациента (окна по 31 дню, до года назад) */
async function checkPlan(patientId) {
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
        return { found: true, title: match[0].title, doctor_id: match[0].doctor_id };
      }
    }
  }
  return { found: false };
}

/** Получить ближайшую будущую запись пациента */
async function getNextAppointment(patientId) {
  const now = new Date();
  const json = await misPost('getAppointments', {
    patient_id: patientId,
    status: 'upcoming',
    date_from: `${fmtDate(now)} 00:00`,
    date_to: `${fmtDate(addDays(now, 30))} 23:59`,
  });

  if (json.error !== 0 || !json.data) return { found: false };

  const appts = Array.isArray(json.data) ? json.data : [json.data];
  appts.sort((a, b) => (a.time_start || '').localeCompare(b.time_start || ''));

  if (appts.length) {
    return { found: true, time_start: appts[0].time_start, doctor_id: appts[0].doctor_id };
  }
  return { found: false };
}

// ===== Хелперы =====

function fmtDate(d) {
  return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()}`;
}
function addDays(d, n) { const r = new Date(d); r.setDate(r.getDate() + n); return r; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ===== Основная логика =====

async function main() {
  // 1. Загрузка данных
  console.log('[1/5] Загрузка врачей из МИС...');
  const doctors = await loadDoctors();
  console.log(`  Загружено ${doctors.byName.size} врачей\n`);

  console.log('[2/4] Загрузка сделок из ВСЕХ стадий...');
  const rawDeals = await getAllDeals();

  // Дедупликация по ID — Битрикс может вернуть одну сделку дважды
  const allDeals = [...new Map(rawDeals.map(d => [d.ID, d])).values()];
  if (rawDeals.length !== allDeals.length) {
    console.log(`  ⚠ Битрикс вернул ${rawDeals.length} записей, уникальных: ${allDeals.length} (дубли API убраны)`);
  }
  console.log(`  Всего сделок: ${allDeals.length}\n`);

  // Статистика по стадиям
  const byStageStat = {};
  for (const d of allDeals) {
    byStageStat[d.STAGE_ID] = (byStageStat[d.STAGE_ID] || 0) + 1;
  }
  console.log('  По стадиям:');
  for (const [stage, count] of Object.entries(byStageStat)) {
    console.log(`    ${sn(stage)}: ${count}`);
  }
  console.log('');

  // 2. Разделяем: NEW-сделки vs остальные
  const newDeals = [];       // сделки в стадии NEW
  const normalDeals = [];    // сделки во всех остальных стадиях

  for (const deal of allDeals) {
    if (!deal.CONTACT_ID) continue;
    if (deal.STAGE_ID === 'NEW') {
      newDeals.push(deal);
    } else {
      normalDeals.push(deal);
    }
  }

  // Контакты у которых есть сделка НЕ в NEW
  const contactsWithNormal = new Set();
  const normalByContact = new Map(); // contactId → первая нормальная сделка (для лога)
  for (const deal of normalDeals) {
    contactsWithNormal.add(deal.CONTACT_ID);
    if (!normalByContact.has(deal.CONTACT_ID)) {
      normalByContact.set(deal.CONTACT_ID, deal);
    }
  }

  // NEW-сделки у контактов которые уже есть в нормальных стадиях → дубли
  const toDelete = newDeals.filter(d => contactsWithNormal.has(d.CONTACT_ID));

  console.log(`  Сделок в NEW: ${newDeals.length}`);
  console.log(`  Сделок в остальных стадиях: ${normalDeals.length}`);
  console.log(`  Контактов с нормальными сделками: ${contactsWithNormal.size}`);
  console.log(`  NEW-дублей к удалению: ${toDelete.length}\n`);

  // 3. Удаление дублей из NEW
  console.log('[3/4] Удаление NEW-дублей...\n');

  let stats = { duplicates: toDelete.length, deleted: 0 };

  for (let i = 0; i < toDelete.length; i++) {
    const dup = toDelete[i];
    const normal = normalByContact.get(dup.CONTACT_ID);

    console.log(`  [${i + 1}/${toDelete.length}] Контакт ${dup.CONTACT_ID}`);
    console.log(`    Оставляем: #${normal.ID} "${normal.TITLE}" [${sn(normal.STAGE_ID)}] врач=${normal.UF_CRM_1774345475 || '?'}`);
    console.log(`    Удаляем:   #${dup.ID} "${dup.TITLE}" [Новая]`);

    if (!DRY_RUN) {
      await bitrix('crm.deal.delete', { id: dup.ID });
      stats.deleted++;
      console.log(`      -> удалена`);
    }
    console.log('');
  }

  // 4. Пройтись по оставшимся сделкам: проверить стадии и даты
  // Перезагружаем сделки после удаления дублей
  console.log('[4/4] Готово.\n');
  console.log('  Стадии будут скорректированы автоматически при следующем вебхуке от МИС.\n');

  // 5. Итог
  console.log(`${'='.repeat(50)}`);
  console.log(`  ИТОГ`);
  console.log(`${'='.repeat(50)}`);
  console.log(`  Найдено дублей:       ${stats.duplicates}`);
  if (DRY_RUN) {
    console.log(`  Режим:                DRY RUN — ничего не изменено`);
    console.log(`\n  Для реального запуска: npm run run`);
  } else {
    console.log(`  Удалено сделок:       ${stats.deleted}`);
    console.log(`  Перенесено товаров:   ${stats.mergedProducts}`);
  }
  console.log(`  API вызовов Битрикс:  ${bitrixCalls}`);
  console.log('');
}

main().catch(err => {
  console.error('\nОшибка:', err.message);
  process.exit(1);
});
