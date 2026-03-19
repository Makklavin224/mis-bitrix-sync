#!/usr/bin/env node

/**
 * Одноразовый импорт визитов из МИС Реновация → Битрикс24
 *
 * Использование:
 *   node sync.js 01.03.2026 19.03.2026
 *
 * Для каждого визита:
 *   1. Определяет сценарий (по статусу + наличию плана лечения)
 *   2. Ищет контакт в Битриксе по телефону
 *   3. Если есть открытые сделки → обновляет (стадия + поля)
 *   4. Если НЕТ открытых сделок → создаёт новую сделку
 */

import 'dotenv/config';

const MIS_BASE = 'https://app.rnova.org/api/public';
const BITRIX_URL = process.env.BITRIX_WEBHOOK_URL;
const MIS_API_KEY = process.env.MIS_API_KEY;

if (!BITRIX_URL || !MIS_API_KEY) {
  console.error('[sync] Ошибка: заполните BITRIX_WEBHOOK_URL и MIS_API_KEY в файле .env');
  process.exit(1);
}

// =====================================================================
//  МИС Реновация API
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

/** Получить все визиты за период */
async function getAppointments(dateFrom, dateTo) {
  const data = await mis('getAppointments', {
    date_from: dateFrom,
    date_to: dateTo,
    show_patient_data: '1',
  });
  return data || [];
}

/** Проверить план лечения пациента */
async function checkTreatmentPlan(patientId, visitDate) {
  if (visitDate) {
    const now = new Date();
    const visit = parseDateDMY(visitDate);
    const start = visit > now ? now : visit;
    return await searchPlan(patientId, formatDateDMY(start), formatDateDMY(addDays(start, 30)));
  }

  // Без visitDate — ищем назад за год
  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));
    const result = await searchPlan(patientId, formatDateDMY(from), formatDateDMY(to));
    if (result.found) return result;
  }

  return { found: false };
}

async function searchPlan(patientId, dateFrom, dateTo) {
  const data = await mis('getPrograms', { date_from: dateFrom, date_to: dateTo });

  if (!data) return { found: false };

  const match = data.filter(p => String(p.patient_id) === String(patientId));
  if (match.length > 0) {
    const plan = match[0];
    return { found: true, title: plan.title || '', doctor_id: plan.doctor_id || null };
  }

  return { found: false };
}

/** Сумма оплаченных счетов за год */
async function getTotalPaid(patientId) {
  let total = 0;
  const now = new Date();

  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));

    const data = await mis('getInvoices', {
      patient_id: patientId,
      status: '2',
      date_from: formatDateDMY(from),
      date_to: formatDateDMY(to),
    });

    if (data) {
      for (const inv of data) total += parseFloat(inv.value) || 0;
    }
  }

  return total;
}

// =====================================================================
//  Шаблоны планов → направление / показание
// =====================================================================

let _templateCache = null;
let _categoryCache = null;

async function loadPlanTemplates() {
  if (_templateCache && _categoryCache) return;

  const cats = await mis('getProgramTemplateCategories');
  _categoryCache = new Map();
  if (cats) {
    (function flatten(items) {
      for (const item of items) {
        _categoryCache.set(String(item.id), item.title.trim());
        if (item.children?.length) flatten(item.children);
      }
    })(cats);
  }

  const tpls = await mis('getProgramTemplates');
  _templateCache = new Map();
  if (tpls) {
    for (const t of tpls) _templateCache.set(t.title, String(t.category_id));
  }

  log(`Загружено ${_categoryCache.size} категорий, ${_templateCache.size} шаблонов планов`);
}

function getDirectionFromPlanTitle(planTitle) {
  if (!_templateCache || !_categoryCache || !planTitle) return null;
  const catId = _templateCache.get(planTitle);
  if (!catId) return null;
  return _categoryCache.get(catId) || null;
}

const INDICATION_MAP = [
  { keywords: ['склеротерапия'], value: 'Склеротерапия' },
  { keywords: ['минифлебэктомия'], value: 'Минифлебэктомии' },
  { keywords: ['ЭВЛК'], value: 'ЭВЛК' },
  { keywords: ['геморроидопластика', 'LHP'], value: 'Лазерная геморроидопластика (LHP)' },
  { keywords: ['геморроидэктомия'], value: 'ГеморроидЭктомия' },
  { keywords: ['свищ', 'фистул'], value: 'Лечение свищей' },
  { keywords: ['склерозирование геморроид'], value: 'Склерозирование геморроидальных узлов' },
  { keywords: ['КЛаКС'], value: 'КЛаКС' },
  { keywords: ['лазерное омоложение'], value: 'Лазерное омоложение' },
  { keywords: ['капельница'], value: 'Капельница' },
  { keywords: ['анализ'], value: 'Анализы' },
  { keywords: ['операцион'], value: 'Операция' },
  { keywords: ['киста'], value: 'Малое инвазивное' },
  { keywords: ['тромбированн'], value: 'Малое инвазивное' },
];

function mapPlanToFields(planTitle, direction) {
  const result = {};
  if (direction) result.direction = direction;

  if (planTitle) {
    const lower = planTitle.toLowerCase();
    for (const entry of INDICATION_MAP) {
      if (entry.keywords.some(kw => lower.includes(kw.toLowerCase()))) {
        result.indication = entry.value;
        break;
      }
    }
  }

  return result;
}

// =====================================================================
//  Битрикс24 API
// =====================================================================

let _bitrixQueue = Promise.resolve();

/** Битрикс24 REST API с авто-тротлингом (макс 2 req/sec) */
async function bitrix(method, params) {
  // Очередь запросов — не более 1 запроса в 500ms
  const result = await new Promise((resolve, reject) => {
    _bitrixQueue = _bitrixQueue.then(async () => {
      await sleep(500);
      try {
        const url = `${BITRIX_URL.replace(/\/$/, '')}/${method}`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(params),
        });

        if (!res.ok) throw new Error(`Bitrix ${method}: ${res.status} ${res.statusText}`);

        const data = await res.json();
        if (data.error) throw new Error(`Bitrix ${method}: ${data.error} — ${data.error_description}`);

        resolve(data);
      } catch (err) {
        reject(err);
      }
    });
  });

  return result;
}

async function findContact(normalizedPhone) {
  const variants = [
    `+${normalizedPhone}`,
    normalizedPhone,
    ...(normalizedPhone.length > 10 ? [normalizedPhone.slice(-10)] : []),
  ];

  for (const phone of variants) {
    const res = await bitrix('crm.contact.list', {
      filter: { PHONE: phone },
      select: ['ID', 'NAME', 'LAST_NAME'],
    });
    if (res?.result?.length) return res.result[0].ID;
  }

  return null;
}

/** Создать контакт в Битриксе из данных МИС */
async function createContact(apt, normalizedPhone) {
  // Разбираем ФИО: "Иванов Иван Иванович" → LAST_NAME, NAME, SECOND_NAME
  const nameParts = (apt.patient_name || '').trim().split(/\s+/);
  const lastName = nameParts[0] || '';
  const firstName = nameParts[1] || '';
  const secondName = nameParts.slice(2).join(' ') || '';

  const fields = {
    NAME: firstName,
    LAST_NAME: lastName,
    PHONE: [{ VALUE: `+${normalizedPhone}`, VALUE_TYPE: 'MOBILE' }],
  };

  if (secondName) fields.SECOND_NAME = secondName;
  if (apt.patient_email) fields.EMAIL = [{ VALUE: apt.patient_email, VALUE_TYPE: 'WORK' }];
  if (apt.patient_birth_date) fields.BIRTHDATE = apt.patient_birth_date;

  const res = await bitrix('crm.contact.add', { fields });
  return res?.result || null;
}

async function findDeals(contactId) {
  const res = await bitrix('crm.deal.list', {
    filter: { CONTACT_ID: contactId, CLOSED: 'N' },
    select: ['ID', 'TITLE', 'STAGE_ID'],
  });
  return res?.result || [];
}

// --- Кэш списочных полей ---

let _dealFieldsCache = null;

async function getDealFieldItems() {
  if (!_dealFieldsCache) {
    const res = await bitrix('crm.deal.fields', {});
    _dealFieldsCache = res?.result || {};
  }
  return _dealFieldsCache;
}

function findListItemId(fieldDef, searchValue) {
  if (!fieldDef?.items || !searchValue) return null;
  const match = fieldDef.items.find(item => item.VALUE === searchValue);
  return match ? match.ID : null;
}

async function findDoctorInList(doctorName) {
  if (!doctorName) return null;

  const fields = await getDealFieldItems();
  const doctorField = fields['UF_CRM_1770366173566'];
  if (!doctorField?.items) return null;

  const parts = doctorName.trim().split(/\s+/);
  const surname = parts[0];

  let match = null;
  if (parts.length >= 2) {
    const initial1 = parts[1]?.replace('.', '')[0];
    match = doctorField.items.find(item => {
      const itemParts = item.VALUE.trim().split(/\s+/);
      return itemParts[0] === surname && itemParts[1]?.[0] === initial1;
    });
  }

  if (!match) {
    match = doctorField.items.find(item => item.VALUE.startsWith(surname));
  }

  return match ? match.ID : null;
}

async function findDirectionId(dirText) {
  const fields = await getDealFieldItems();
  return findListItemId(fields['UF_CRM_1770364332145'], dirText);
}

async function findIndicationId(indText) {
  const fields = await getDealFieldItems();
  return findListItemId(fields['UF_CRM_1770364702542'], indText);
}

async function setDealProducts(dealId, services) {
  if (!services?.length) return;

  const rows = services
    .filter(s => s.title && s.price)
    .map(s => ({
      PRODUCT_NAME: s.title,
      PRICE: parseFloat(s.price) || 0,
      QUANTITY: parseInt(s.count) || 1,
    }));

  if (!rows.length) return;

  await bitrix('crm.deal.productrows.set', { id: dealId, rows });
}

// =====================================================================
//  Сборка полей сделки
// =====================================================================

async function buildDealFields(targetStage, apt, planData, doctorListId, includePlanFields) {
  const fields = {};

  if (apt.time_start) {
    fields['UF_CRM_1769787005282'] = apt.time_start;
    if (targetStage === 'UC_JLJ6EI') {
      fields['UF_CRM_1769787080767'] = apt.time_start;
    }
  }
  if (apt.patient_name) fields['UF_CRM_1770364627343'] = apt.patient_name;
  if (apt.patient_birth_date) fields['UF_CRM_1772184930463'] = apt.patient_birth_date;
  if (doctorListId) fields['UF_CRM_1770366173566'] = doctorListId;

  if (includePlanFields && planData?.found) {
    const mapped = mapPlanToFields(planData.title, planData.direction);

    if (mapped.direction) {
      const dirId = await findDirectionId(mapped.direction);
      if (dirId) fields['UF_CRM_1770364332145'] = dirId;
    }
    if (mapped.indication) {
      const indId = await findIndicationId(mapped.indication);
      if (indId) fields['UF_CRM_1770364702542'] = indId;
    }
  }

  return fields;
}

// =====================================================================
//  Определение сценария
// =====================================================================

async function determineScenario(apt) {
  const status = apt.status;
  const isFirst = apt.is_first === true || apt.is_first === '1' || apt.is_first === 1
    || apt.type === 'Первичный прием';

  let targetStage, scenario;
  let planData = { found: false };

  if (status === 'upcoming') {
    // Как create.js
    if (isFirst) {
      targetStage = 'FINAL_INVOICE';
      scenario = 'new_first_visit';
    } else {
      planData = await checkTreatmentPlan(apt.patient_id);
      if (planData.found) {
        targetStage = 'UC_JLJ6EI';
        scenario = 'new_appointment_with_plan';
      } else {
        targetStage = 'FINAL_INVOICE';
        scenario = 'returning_no_plan';
      }
    }
  } else if (status === 'completed') {
    // Как update.js (completed)
    const visitDate = apt.time_start?.split(' ')[0];
    planData = await checkTreatmentPlan(apt.patient_id, visitDate);
    if (planData.found) {
      targetStage = 'UC_7KB49S';
      scenario = 'completed_with_plan';
    } else {
      targetStage = 'UC_LVYHC1';
      scenario = 'completed_no_plan';
    }
  } else if (status === 'refused') {
    // Как update.js (refused)
    planData = await checkTreatmentPlan(apt.patient_id);
    if (planData.found) {
      targetStage = 'UC_QAU8BB';
      scenario = 'noshow_with_plan';
    } else {
      targetStage = 'UC_1HMFHN';
      scenario = 'noshow_no_plan';
    }
  } else {
    return null; // неизвестный статус — пропускаем
  }

  return { targetStage, scenario, planData };
}

// =====================================================================
//  Обработка одного визита
// =====================================================================

async function syncOne(apt) {
  const rawPhone = apt.patient_phone;
  if (!rawPhone) return { status: 'skip', reason: 'no_phone' };

  const digits = rawPhone.replace(/\D/g, '');
  const normalized = digits.startsWith('8') ? '7' + digits.slice(1) : digits;

  if (normalized.length < 10) return { status: 'skip', reason: 'short_phone' };

  // 1. Сценарий
  const scenarioResult = await determineScenario(apt);
  if (!scenarioResult) return { status: 'skip', reason: `status_${apt.status}` };

  const { targetStage, scenario, planData } = scenarioResult;

  // 2. Направление из шаблона плана
  if (planData.found) {
    await loadPlanTemplates();
    planData.direction = getDirectionFromPlanTitle(planData.title);
  }

  // 3. Контакт в Битриксе — ищем или создаём
  let contactId = await findContact(normalized);
  let contactCreated = false;

  if (!contactId) {
    contactId = await createContact(apt, normalized);
    if (!contactId) return { status: 'skip', reason: 'contact_create_failed', phone: `+${normalized}` };
    contactCreated = true;
  }

  // 4. Врач
  const doctorListId = await findDoctorInList(apt.doctor);

  // 5. Проверка совпадения врача с планом
  const isProcedureRoom = (apt.doctor || '').toLowerCase().includes('процедурный кабинет');
  const isSameDoctor = !planData.found || !planData.doctor_id || !apt.doctor_id
    || String(planData.doctor_id) === String(apt.doctor_id)
    || isProcedureRoom;
  const includePlanFields = planData.found && isSameDoctor;

  // 6. Доп. поля
  const extraFields = await buildDealFields(targetStage, apt, planData, doctorListId, includePlanFields);

  // 7. Сумма оплат
  const totalPaid = await getTotalPaid(apt.patient_id);
  if (totalPaid > 0) extraFields['UF_CRM_1770381244477'] = String(totalPaid);

  // 8. Открытые сделки
  const deals = await findDeals(contactId);

  if (deals.length === 0) {
    // === НЕТ СДЕЛОК → СОЗДАЁМ ===
    const res = await bitrix('crm.deal.add', {
      fields: {
        STAGE_ID: targetStage,
        CONTACT_ID: contactId,
        TITLE: `${apt.patient_name || 'Пациент'} — ${scenario}`,
        ...extraFields,
      },
    });

    const dealId = res?.result;

    if (apt.services?.length && dealId) {
      await setDealProducts(dealId, apt.services);
    }

    return { status: 'created', dealId, stage: targetStage, scenario, contactCreated, contactId };
  }

  // === ЕСТЬ СДЕЛКИ → ОБНОВЛЯЕМ ===
  const results = [];
  for (const deal of deals) {
    if (deal.STAGE_ID === targetStage) {
      // Уже на нужной стадии — обновляем только поля
      if (Object.keys(extraFields).length) {
        await bitrix('crm.deal.update', { id: deal.ID, fields: extraFields });
      }
      results.push({ dealId: deal.ID, action: 'already_at_stage' });
      continue;
    }

    await bitrix('crm.deal.update', {
      id: deal.ID,
      fields: { STAGE_ID: targetStage, ...extraFields },
    });

    if (apt.services?.length) {
      await setDealProducts(deal.ID, apt.services);
    }

    results.push({ dealId: deal.ID, action: 'moved', from: deal.STAGE_ID, to: targetStage });
  }

  return { status: 'updated', deals: results, scenario };
}

// =====================================================================
//  Главный цикл
// =====================================================================

async function main() {
  const [dateFrom, dateTo] = process.argv.slice(2);

  if (!dateFrom || !dateTo) {
    console.log('');
    console.log('  Импорт визитов из МИС Реновация → Битрикс24');
    console.log('');
    console.log('  Использование:');
    console.log('    node sync.js ДД.ММ.ГГГГ ДД.ММ.ГГГГ');
    console.log('');
    console.log('  Пример:');
    console.log('    node sync.js 01.03.2026 19.03.2026');
    console.log('');
    process.exit(1);
  }

  log(`Запуск синхронизации: ${dateFrom} → ${dateTo}`);
  log('');

  // 1. Получаем все визиты из МИС
  log('Загрузка визитов из МИС...');
  const appointments = await getAppointments(dateFrom, dateTo);
  log(`Найдено визитов: ${appointments.length}`);

  if (!appointments.length) {
    log('Нет визитов для обработки.');
    return;
  }

  // 2. Сортируем по дате (хронологически)
  appointments.sort((a, b) => {
    const tA = a.time_start || '';
    const tB = b.time_start || '';
    return tA.localeCompare(tB);
  });

  // 3. Фильтруем только нужные статусы
  const relevant = appointments.filter(a =>
    a.status === 'upcoming' || a.status === 'completed' || a.status === 'refused'
  );
  log(`Релевантных (upcoming/completed/refused): ${relevant.length}`);
  log('');

  // 4. Обрабатываем
  const stats = {
    total: relevant.length,
    created: 0,
    updated: 0,
    already: 0,
    skipped: 0,
    errors: 0,
    contactsCreated: 0,
  };

  const errors = [];

  for (let i = 0; i < relevant.length; i++) {
    const apt = relevant[i];
    const num = `[${i + 1}/${relevant.length}]`;
    const name = apt.patient_name || 'Неизвестный';
    const phone = apt.patient_phone || '—';

    try {
      const result = await syncOne(apt);

      if (result.status === 'created') {
        stats.created++;
        if (result.contactCreated) stats.contactsCreated++;
        const contactNote = result.contactCreated ? ` (+ новый контакт #${result.contactId})` : '';
        log(`${num} ${name} (${phone}) — ${apt.status} → СОЗДАНА сделка #${result.dealId} [${result.scenario}]${contactNote}`);
      } else if (result.status === 'updated') {
        const actions = result.deals.map(d => {
          if (d.action === 'already_at_stage') return `#${d.dealId} уже на месте`;
          return `#${d.dealId}: ${d.from} → ${d.to}`;
        }).join(', ');

        const hasRealUpdate = result.deals.some(d => d.action !== 'already_at_stage');
        if (hasRealUpdate) {
          stats.updated++;
          log(`${num} ${name} (${phone}) — ${apt.status} → ОБНОВЛЕНО: ${actions}`);
        } else {
          stats.already++;
          log(`${num} ${name} (${phone}) — ${apt.status} → уже актуально: ${actions}`);
        }
      } else if (result.status === 'skip') {
        stats.skipped++;
        if (result.reason === 'no_contact') {
          skippedNoContact.push({ name, phone: result.phone || phone });
        }
      }
    } catch (err) {
      stats.errors++;
      errors.push({ name, phone, error: err.message });
      log(`${num} ${name} (${phone}) — ОШИБКА: ${err.message}`);
    }
  }

  // 5. Итоги
  log('');
  log('═══════════════════════════════════════');
  log('  ИТОГИ СИНХРОНИЗАЦИИ');
  log('═══════════════════════════════════════');
  log(`  Всего визитов:      ${stats.total}`);
  log(`  Создано сделок:     ${stats.created}`);
  log(`  Создано контактов:  ${stats.contactsCreated}`);
  log(`  Обновлено сделок:   ${stats.updated}`);
  log(`  Уже актуально:      ${stats.already}`);
  log(`  Пропущено:          ${stats.skipped}`);
  log(`  Ошибки:             ${stats.errors}`);
  log('═══════════════════════════════════════');

  if (errors.length) {
    log('');
    log(`Ошибки (${errors.length}):`);
    for (const { name, phone, error } of errors) {
      log(`  - ${name} (${phone}): ${error}`);
    }
  }
}

// =====================================================================
//  Хелперы
// =====================================================================

function parseDateDMY(str) {
  const [d, m, y] = str.split('.');
  return new Date(parseInt(y), parseInt(m) - 1, parseInt(d));
}

function formatDateDMY(date) {
  const d = String(date.getDate()).padStart(2, '0');
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const y = date.getFullYear();
  return `${d}.${m}.${y}`;
}

function addDays(date, days) {
  const r = new Date(date);
  r.setDate(r.getDate() + days);
  return r;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function log(msg) {
  const ts = new Date().toLocaleTimeString('ru-RU');
  console.log(`[${ts}] ${msg}`);
}

// =====================================================================
//  Запуск
// =====================================================================

main().catch(err => {
  console.error('[sync] Критическая ошибка:', err);
  process.exit(1);
});
