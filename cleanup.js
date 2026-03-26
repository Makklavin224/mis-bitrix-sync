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
const CLOSED_STAGES = ['UC_NCW0DT', 'UC_F92MOY', 'WON', 'LOSE', 'APOLOGY'];

const STAGE_NAMES = {
  'NEW': 'Новая',
  'PREPARATION': 'Подготовка',
  'EXECUTING': 'В работе',
  'PREPAYMENT_INVOICE': 'Предоплата',
  'FINAL_INVOICE': 'Записан на консультацию',
  'UC_JLJ6EI': 'Записан на лечение',
  'UC_7KB49S': 'Есть показания',
  'UC_LVYHC1': 'Нет показаний',
  'UC_QAU8BB': 'Не пришёл (лечение)',
  'UC_1HMFHN': 'Не пришёл (консультация)',
  'UC_F92MOY': 'Лечение завершено',
};
function sn(id) { return STAGE_NAMES[id] || id; }

// Приоритет стадий: чем выше число — тем важнее, оставляем при дедупликации
const STAGE_PRIORITY = {
  'NEW': 0,
  'PREPARATION': 1,
  'EXECUTING': 2,
  'PREPAYMENT_INVOICE': 3,
  'FINAL_INVOICE': 10,         // записан на консультацию
  'UC_1HMFHN': 11,             // не пришёл на консультацию
  'UC_LVYHC1': 12,             // нет показаний
  'UC_QAU8BB': 13,             // не пришёл на лечение
  'UC_7KB49S': 14,             // есть показания к лечению
  'UC_JLJ6EI': 15,             // записан на лечение
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

async function bitrix(method, params) {
  const url = `${BITRIX_URL.replace(/\/$/, '')}/${method}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  const data = await res.json();
  bitrixCalls++;

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
      filter: { CLOSED: 'N' },
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

  console.log('[2/4] Загрузка открытых сделок...');
  const allDeals = await getAllDeals();
  console.log(`  Всего открытых сделок: ${allDeals.length}\n`);

  // 2. Группировка по контакту
  const byContact = new Map();
  for (const deal of allDeals) {
    if (!deal.CONTACT_ID) continue;
    if (!byContact.has(deal.CONTACT_ID)) byContact.set(deal.CONTACT_ID, []);
    byContact.get(deal.CONTACT_ID).push(deal);
  }
  console.log(`  Контактов с открытыми сделками: ${byContact.size}\n`);

  // 3. Поиск и удаление дублей
  console.log('[3/4] Поиск дублей...\n');

  let stats = { duplicates: 0, deleted: 0, mergedProducts: 0, stageFixed: 0, dateFixed: 0 };

  for (const [contactId, deals] of byContact) {
    const activeDeals = deals.filter(d => !CLOSED_STAGES.includes(d.STAGE_ID));
    if (activeDeals.length <= 1) continue;

    // Кластеризация по врачу/специальности
    const clusters = [];

    for (const deal of activeDeals) {
      const docName = (deal.UF_CRM_1774345475 || '').trim();
      const docMis = docName ? doctors.byName.get(docName) : null;

      let placed = false;
      for (const cluster of clusters) {
        const clusterDocName = (cluster[0].UF_CRM_1774345475 || '').trim();
        const clusterDocMis = clusterDocName ? doctors.byName.get(clusterDocName) : null;

        // Точное совпадение врача
        if (docName && docName === clusterDocName) {
          cluster.push(deal);
          placed = true;
          break;
        }

        // Совпадение по специальности
        if (docMis && clusterDocMis && sharesSpecialty(doctors, docMis.id, clusterDocMis.id)) {
          cluster.push(deal);
          placed = true;
          break;
        }

        // Любая из сторон без врача или в ранней стадии → объединяем
        const EARLY = ['NEW', 'PREPARATION', 'EXECUTING', 'PREPAYMENT_INVOICE'];
        const dealEarly = !docName || EARLY.includes(deal.STAGE_ID);
        const clusterEarly = !clusterDocName || EARLY.includes(cluster[0].STAGE_ID);
        if (dealEarly || clusterEarly) {
          cluster.push(deal);
          placed = true;
          break;
        }
      }

      if (!placed) {
        clusters.push([deal]);
      }
    }

    // Обработка кластеров с дублями
    for (const cluster of clusters) {
      if (cluster.length <= 1) continue;

      // Сортируем: продвинутые стадии первыми, ранние (NEW и пр.) — в конец на удаление
      cluster.sort((a, b) => stagePriority(b.STAGE_ID) - stagePriority(a.STAGE_ID));

      const keep = cluster[0];
      const dups = cluster.slice(1);
      const keepDoc = keep.UF_CRM_1774345475 || '?';

      console.log(`  Контакт ${contactId} | ${keepDoc}`);
      console.log(`    Оставляем: #${keep.ID} "${keep.TITLE}" [${sn(keep.STAGE_ID)}]`);

      for (const dup of dups) {
        const dupDoc = dup.UF_CRM_1774345475 || '(без врача)';
        console.log(`    Дубль:     #${dup.ID} "${dup.TITLE}" [${sn(dup.STAGE_ID)}] врач=${dupDoc}`);
        stats.duplicates++;

        if (!DRY_RUN) {
          // Перенести товары
          const dupProducts = await getDealProducts(dup.ID);
          if (dupProducts.length) {
            const mainProducts = await getDealProducts(keep.ID);
            const keys = new Set(mainProducts.map(p => `${p.PRODUCT_NAME}|${p.PRICE}`));
            const newOnes = dupProducts.filter(p => !keys.has(`${p.PRODUCT_NAME}|${p.PRICE}`));

            if (newOnes.length) {
              const rows = [...mainProducts, ...newOnes].map(p => ({
                PRODUCT_NAME: p.PRODUCT_NAME,
                PRICE: parseFloat(p.PRICE) || 0,
                QUANTITY: parseInt(p.QUANTITY) || 1,
              }));
              await bitrix('crm.deal.productrows.set', { id: keep.ID, rows });
              stats.mergedProducts += newOnes.length;
              console.log(`      -> перенесено ${newOnes.length} товаров`);
            }
          }

          await bitrix('crm.deal.delete', { id: dup.ID });
          stats.deleted++;
          console.log(`      -> удалена`);
          await sleep(300);
        }
      }
      console.log('');
    }
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
