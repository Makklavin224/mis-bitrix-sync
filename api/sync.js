/**
 * POST /api/sync?date_from=01.03.2026&date_to=19.03.2026
 * POST /api/sync?date_from=01.03.2026&date_to=19.03.2026&offset=100&limit=100
 *
 * Импорт визитов из МИС Реновация → Битрикс24
 *
 * Пагинация: offset + limit (по умолчанию 0 + 200).
 * Если записей больше чем limit — в ответе будет next_offset для следующего вызова.
 *
 * Услуги: НАКАПЛИВАЮТСЯ по всем визитам пациента (не перезаписываются).
 * Оплата: getTotalPaid всегда берёт актуальную сумму из МИС.
 */

const MIS_BASE = 'https://app.rnova.org/api/public';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'POST only' });
  }

  const BITRIX_URL = process.env.BITRIX_WEBHOOK_URL;
  const MIS_API_KEY = process.env.MIS_API_KEY;

  if (!BITRIX_URL || !MIS_API_KEY) {
    return res.status(500).json({ error: 'BITRIX_WEBHOOK_URL or MIS_API_KEY not set' });
  }

  const dateFrom = req.query.date_from || req.body?.date_from;
  const dateTo = req.query.date_to || req.body?.date_to;
  const offset = parseInt(req.query.offset || '0') || 0;
  const limit = parseInt(req.query.limit || '200') || 200;

  if (!dateFrom || !dateTo) {
    return res.status(400).json({
      error: 'date_from and date_to required',
      example: 'POST /api/sync?date_from=01.03.2026&date_to=19.03.2026&offset=0&limit=200',
    });
  }

  console.log(`[sync] start: ${dateFrom} → ${dateTo}, offset=${offset}, limit=${limit}`);

  try {
    const result = await runSync(BITRIX_URL, MIS_API_KEY, dateFrom, dateTo, offset, limit);
    return res.status(200).json(result);
  } catch (err) {
    console.error('[sync] critical error:', err.message);
    return res.status(200).json({ error: err.message });
  }
}

// =====================================================================
//  Главный цикл
// =====================================================================

async function runSync(BITRIX_URL, MIS_API_KEY, dateFrom, dateTo, offset, limit) {
  const ctx = { BITRIX_URL, MIS_API_KEY, _dealFieldsCache: null, _templateCache: null, _categoryCache: null };

  // 1. Визиты из МИС
  console.log('[sync] fetching appointments...');
  const allAppointments = await getAppointments(ctx, dateFrom, dateTo);
  console.log(`[sync] total from MIS: ${allAppointments.length}`);

  if (!allAppointments.length) {
    return { ok: true, message: 'no appointments', stats: {} };
  }

  // 2. Хронологический порядок
  allAppointments.sort((a, b) => (a.time_start || '').localeCompare(b.time_start || ''));

  // 3. Только нужные статусы
  const allRelevant = allAppointments.filter(a =>
    a.status === 'upcoming' || a.status === 'completed' || a.status === 'refused'
  );

  // 4. Пагинация
  const batch = allRelevant.slice(offset, offset + limit);
  const hasMore = offset + limit < allRelevant.length;

  console.log(`[sync] relevant: ${allRelevant.length}, batch: ${offset}..${offset + batch.length}`);

  // 5. Группируем визиты по пациенту (patient_id) — чтобы собрать ВСЕ услуги
  const byPatient = new Map();
  for (const apt of allRelevant) {
    const pid = apt.patient_id;
    if (!pid) continue;
    if (!byPatient.has(pid)) byPatient.set(pid, []);
    byPatient.get(pid).push(apt);
  }

  // 6. Обработка батча
  const stats = { total_in_mis: allRelevant.length, batch_offset: offset, batch_size: batch.length, created: 0, updated: 0, already: 0, skipped: 0, errors: 0, contactsCreated: 0 };
  const log = [];
  const errors = [];

  for (let i = 0; i < batch.length; i++) {
    const apt = batch[i];
    const name = apt.patient_name || '?';
    const phone = apt.patient_phone || '—';

    try {
      // Собираем ВСЕ услуги по всем визитам этого пациента
      const patientVisits = byPatient.get(apt.patient_id) || [];
      const allServices = [];
      for (const v of patientVisits) {
        if (v.services?.length) allServices.push(...v.services);
      }

      const result = await syncOne(ctx, apt, allServices);

      if (result.status === 'created') {
        stats.created++;
        if (result.contactCreated) stats.contactsCreated++;
        log.push({ i: offset + i + 1, name, phone, action: 'created', dealId: result.dealId, scenario: result.scenario, contactCreated: result.contactCreated || false });
        console.log(`[sync] ${offset + i + 1} ${name} → CREATED #${result.dealId} [${result.scenario}]${result.contactCreated ? ' +contact' : ''}`);
      } else if (result.status === 'updated') {
        const hasRealUpdate = result.deals.some(d => d.action !== 'already_at_stage');
        if (hasRealUpdate) {
          stats.updated++;
          log.push({ i: offset + i + 1, name, phone, action: 'updated', deals: result.deals, scenario: result.scenario });
        } else {
          stats.already++;
          log.push({ i: offset + i + 1, name, phone, action: 'already', deals: result.deals });
        }
        console.log(`[sync] ${offset + i + 1} ${name} → ${hasRealUpdate ? 'UPDATED' : 'ok'}`);
      } else if (result.status === 'skip') {
        stats.skipped++;
        log.push({ i: offset + i + 1, name, phone, action: 'skipped', reason: result.reason });
      }
    } catch (err) {
      stats.errors++;
      errors.push({ i: offset + i + 1, name, phone, error: err.message });
      console.error(`[sync] ${offset + i + 1} ${name} — ERROR: ${err.message}`);
    }
  }

  console.log(`[sync] batch done: created=${stats.created}, updated=${stats.updated}, already=${stats.already}, skipped=${stats.skipped}, errors=${stats.errors}`);

  const result = { ok: true, stats, errors };

  if (hasMore) {
    result.next_offset = offset + limit;
    result.next_url = `/api/sync?date_from=${dateFrom}&date_to=${dateTo}&offset=${offset + limit}&limit=${limit}`;
  } else {
    result.complete = true;
  }

  // Не отдаём лог целиком если > 50 записей (тяжело)
  if (log.length <= 50) result.log = log;

  return result;
}

// =====================================================================
//  Обработка одного визита
// =====================================================================

async function syncOne(ctx, apt, allPatientServices) {
  const rawPhone = apt.patient_phone;
  if (!rawPhone) return { status: 'skip', reason: 'no_phone' };

  const digits = rawPhone.replace(/\D/g, '');
  const normalized = digits.startsWith('8') ? '7' + digits.slice(1) : digits;
  if (normalized.length < 10) return { status: 'skip', reason: 'short_phone' };

  // 1. Сценарий
  const scenarioResult = await determineScenario(ctx, apt);
  if (!scenarioResult) return { status: 'skip', reason: `status_${apt.status}` };

  const { targetStage, scenario, planData } = scenarioResult;

  // 2. Направление из шаблона
  if (planData.found) {
    await loadPlanTemplates(ctx);
    planData.direction = getDirectionFromPlanTitle(ctx, planData.title);
  }

  // 3. Контакт — ищем или создаём
  let contactId = await findContact(ctx, normalized);
  let contactCreated = false;

  if (!contactId) {
    contactId = await createContact(ctx, apt, normalized);
    if (!contactId) return { status: 'skip', reason: 'contact_create_failed' };
    contactCreated = true;
  }

  // 4. Врач
  const doctorListId = await findDoctorInList(ctx, apt.doctor);

  // 5. Совпадение врача с планом
  const isProcedureRoom = (apt.doctor || '').toLowerCase().includes('процедурный кабинет');
  const isSameDoctor = !planData.found || !planData.doctor_id || !apt.doctor_id
    || String(planData.doctor_id) === String(apt.doctor_id)
    || isProcedureRoom;
  const includePlanFields = planData.found && isSameDoctor;

  // 6. Поля сделки
  const extraFields = await buildDealFields(ctx, targetStage, apt, planData, doctorListId, includePlanFields);

  // 7. Сумма оплат — АКТУАЛЬНАЯ из МИС (за все визиты)
  const totalPaid = await getTotalPaid(ctx, apt.patient_id);
  if (totalPaid > 0) extraFields['UF_CRM_1770381244477'] = String(totalPaid);

  // 8. Сделки
  const deals = await findDeals(ctx, contactId);

  if (deals.length === 0) {
    const r = await bitrix(ctx, 'crm.deal.add', {
      fields: { STAGE_ID: targetStage, CONTACT_ID: contactId, TITLE: `${apt.patient_name || 'Пациент'} — ${scenario}`, ...extraFields },
    });
    const dealId = r?.result;
    // Услуги — ВСЕ по пациенту (итоговые)
    if (allPatientServices.length && dealId) await setDealProducts(ctx, dealId, allPatientServices);
    return { status: 'created', dealId, stage: targetStage, scenario, contactCreated, contactId };
  }

  const results = [];
  for (const deal of deals) {
    if (deal.STAGE_ID === targetStage) {
      if (Object.keys(extraFields).length) {
        await bitrix(ctx, 'crm.deal.update', { id: deal.ID, fields: extraFields });
      }
      // Услуги — обновляем итоговые даже если стадия не менялась
      if (allPatientServices.length) await setDealProducts(ctx, deal.ID, allPatientServices);
      results.push({ dealId: deal.ID, action: 'already_at_stage' });
      continue;
    }
    await bitrix(ctx, 'crm.deal.update', { id: deal.ID, fields: { STAGE_ID: targetStage, ...extraFields } });
    if (allPatientServices.length) await setDealProducts(ctx, deal.ID, allPatientServices);
    results.push({ dealId: deal.ID, action: 'moved', from: deal.STAGE_ID, to: targetStage });
  }

  return { status: 'updated', deals: results, scenario };
}

// =====================================================================
//  Определение сценария
// =====================================================================

async function determineScenario(ctx, apt) {
  const status = apt.status;
  const isFirst = apt.is_first === true || apt.is_first === '1' || apt.is_first === 1 || apt.type === 'Первичный прием';

  let targetStage, scenario;
  let planData = { found: false };

  if (status === 'upcoming') {
    if (isFirst) {
      targetStage = 'FINAL_INVOICE'; scenario = 'new_first_visit';
    } else {
      planData = await checkTreatmentPlan(ctx, apt.patient_id);
      targetStage = planData.found ? 'UC_JLJ6EI' : 'FINAL_INVOICE';
      scenario = planData.found ? 'new_appointment_with_plan' : 'returning_no_plan';
    }
  } else if (status === 'completed') {
    const visitDate = apt.time_start?.split(' ')[0];
    planData = await checkTreatmentPlan(ctx, apt.patient_id, visitDate);
    targetStage = planData.found ? 'UC_7KB49S' : 'UC_LVYHC1';
    scenario = planData.found ? 'completed_with_plan' : 'completed_no_plan';
  } else if (status === 'refused') {
    planData = await checkTreatmentPlan(ctx, apt.patient_id);
    targetStage = planData.found ? 'UC_QAU8BB' : 'UC_1HMFHN';
    scenario = planData.found ? 'noshow_with_plan' : 'noshow_no_plan';
  } else {
    return null;
  }

  return { targetStage, scenario, planData };
}

// =====================================================================
//  МИС Реновация API
// =====================================================================

async function mis(ctx, method, params = {}) {
  const body = new URLSearchParams({ api_key: ctx.MIS_API_KEY, ...params });
  const res = await fetch(`${MIS_BASE}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });
  const json = await res.json();
  if (json.error !== 0 || !json.data) return null;
  return Array.isArray(json.data) ? json.data : [json.data];
}

async function getAppointments(ctx, dateFrom, dateTo) {
  return (await mis(ctx, 'getAppointments', { date_from: dateFrom, date_to: dateTo, show_patient_data: '1' })) || [];
}

async function checkTreatmentPlan(ctx, patientId, visitDate) {
  if (visitDate) {
    const now = new Date();
    const visit = parseDateDMY(visitDate);
    const start = visit > now ? now : visit;
    return await searchPlan(ctx, patientId, formatDateDMY(start), formatDateDMY(addDays(start, 30)));
  }
  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));
    const result = await searchPlan(ctx, patientId, formatDateDMY(from), formatDateDMY(to));
    if (result.found) return result;
  }
  return { found: false };
}

async function searchPlan(ctx, patientId, dateFrom, dateTo) {
  const data = await mis(ctx, 'getPrograms', { date_from: dateFrom, date_to: dateTo });
  if (!data) return { found: false };
  const match = data.filter(p => String(p.patient_id) === String(patientId));
  if (match.length > 0) {
    return { found: true, title: match[0].title || '', doctor_id: match[0].doctor_id || null };
  }
  return { found: false };
}

async function getTotalPaid(ctx, patientId) {
  let total = 0;
  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const to = addDays(now, -(i * 31));
    const from = addDays(now, -((i + 1) * 31));
    const data = await mis(ctx, 'getInvoices', { patient_id: patientId, status: '2', date_from: formatDateDMY(from), date_to: formatDateDMY(to) });
    if (data) for (const inv of data) total += parseFloat(inv.value) || 0;
  }
  return total;
}

// =====================================================================
//  Шаблоны планов → направление / показание
// =====================================================================

async function loadPlanTemplates(ctx) {
  if (ctx._templateCache && ctx._categoryCache) return;

  const cats = await mis(ctx, 'getProgramTemplateCategories');
  ctx._categoryCache = new Map();
  if (cats) {
    (function flatten(items) {
      for (const item of items) {
        ctx._categoryCache.set(String(item.id), item.title.trim());
        if (item.children?.length) flatten(item.children);
      }
    })(cats);
  }

  const tpls = await mis(ctx, 'getProgramTemplates');
  ctx._templateCache = new Map();
  if (tpls) for (const t of tpls) ctx._templateCache.set(t.title, String(t.category_id));

  console.log(`[sync] loaded ${ctx._categoryCache.size} categories, ${ctx._templateCache.size} templates`);
}

function getDirectionFromPlanTitle(ctx, planTitle) {
  if (!ctx._templateCache || !ctx._categoryCache || !planTitle) return null;
  const catId = ctx._templateCache.get(planTitle);
  if (!catId) return null;
  return ctx._categoryCache.get(catId) || null;
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
      if (entry.keywords.some(kw => lower.includes(kw.toLowerCase()))) { result.indication = entry.value; break; }
    }
  }
  return result;
}

// =====================================================================
//  Битрикс24 API — с retry при rate limit
// =====================================================================

async function bitrix(ctx, method, params) {
  const url = `${ctx.BITRIX_URL.replace(/\/$/, '')}/${method}`;

  for (let attempt = 0; attempt < 5; attempt++) {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
    });

    if (!r.ok) throw new Error(`Bitrix ${method}: ${r.status}`);

    const data = await r.json();

    // Rate limit — ждём и повторяем
    if (data.error === 'QUERY_LIMIT_EXCEEDED') {
      console.log(`[bitrix] rate limit on ${method}, waiting ${(attempt + 1) * 2}s...`);
      await sleep((attempt + 1) * 2000);
      continue;
    }

    if (data.error) throw new Error(`Bitrix ${method}: ${data.error} — ${data.error_description}`);

    return data;
  }

  throw new Error(`Bitrix ${method}: rate limit exceeded after 5 retries`);
}

async function findContact(ctx, normalizedPhone) {
  const variants = [`+${normalizedPhone}`, normalizedPhone, ...(normalizedPhone.length > 10 ? [normalizedPhone.slice(-10)] : [])];
  for (const phone of variants) {
    const res = await bitrix(ctx, 'crm.contact.list', { filter: { PHONE: phone }, select: ['ID', 'NAME', 'LAST_NAME'] });
    if (res?.result?.length) return res.result[0].ID;
  }
  return null;
}

async function createContact(ctx, apt, normalizedPhone) {
  const nameParts = (apt.patient_name || '').trim().split(/\s+/);
  const fields = {
    NAME: nameParts[1] || '',
    LAST_NAME: nameParts[0] || '',
    PHONE: [{ VALUE: `+${normalizedPhone}`, VALUE_TYPE: 'MOBILE' }],
  };
  const secondName = nameParts.slice(2).join(' ');
  if (secondName) fields.SECOND_NAME = secondName;
  if (apt.patient_email) fields.EMAIL = [{ VALUE: apt.patient_email, VALUE_TYPE: 'WORK' }];
  if (apt.patient_birth_date) fields.BIRTHDATE = apt.patient_birth_date;

  const res = await bitrix(ctx, 'crm.contact.add', { fields });
  return res?.result || null;
}

async function findDeals(ctx, contactId) {
  const res = await bitrix(ctx, 'crm.deal.list', { filter: { CONTACT_ID: contactId, CLOSED: 'N' }, select: ['ID', 'TITLE', 'STAGE_ID'] });
  return res?.result || [];
}

async function getDealFieldItems(ctx) {
  if (!ctx._dealFieldsCache) {
    const res = await bitrix(ctx, 'crm.deal.fields', {});
    ctx._dealFieldsCache = res?.result || {};
  }
  return ctx._dealFieldsCache;
}

function findListItemId(fieldDef, searchValue) {
  if (!fieldDef?.items || !searchValue) return null;
  const match = fieldDef.items.find(item => item.VALUE === searchValue);
  return match ? match.ID : null;
}

async function findDoctorInList(ctx, doctorName) {
  if (!doctorName) return null;
  const fields = await getDealFieldItems(ctx);
  const doctorField = fields['UF_CRM_1770366173566'];
  if (!doctorField?.items) return null;

  const parts = doctorName.trim().split(/\s+/);
  const surname = parts[0];

  let match = null;
  if (parts.length >= 2) {
    const init = parts[1]?.replace('.', '')[0];
    match = doctorField.items.find(item => { const p = item.VALUE.trim().split(/\s+/); return p[0] === surname && p[1]?.[0] === init; });
  }
  if (!match) match = doctorField.items.find(item => item.VALUE.startsWith(surname));
  return match ? match.ID : null;
}

async function findDirectionId(ctx, dirText) { return findListItemId((await getDealFieldItems(ctx))['UF_CRM_1770364332145'], dirText); }
async function findIndicationId(ctx, indText) { return findListItemId((await getDealFieldItems(ctx))['UF_CRM_1770364702542'], indText); }

async function setDealProducts(ctx, dealId, services) {
  if (!services?.length) return;

  // Дедупликация: одинаковые услуги (по title) → одна строка, количество суммируется
  const map = new Map();
  for (const s of services) {
    if (!s.title || !s.price) continue;
    const key = s.title;
    if (map.has(key)) {
      map.get(key).qty += parseInt(s.count) || 1;
    } else {
      map.set(key, { title: s.title, price: parseFloat(s.price) || 0, qty: parseInt(s.count) || 1 });
    }
  }

  const rows = [...map.values()].map(s => ({ PRODUCT_NAME: s.title, PRICE: s.price, QUANTITY: s.qty }));
  if (rows.length) await bitrix(ctx, 'crm.deal.productrows.set', { id: dealId, rows });
}

async function buildDealFields(ctx, targetStage, apt, planData, doctorListId, includePlanFields) {
  const fields = {};
  if (apt.time_start) {
    fields['UF_CRM_1769787005282'] = apt.time_start;
    if (targetStage === 'UC_JLJ6EI') fields['UF_CRM_1769787080767'] = apt.time_start;
  }
  if (apt.patient_name) fields['UF_CRM_1770364627343'] = apt.patient_name;
  if (apt.patient_birth_date) fields['UF_CRM_1772184930463'] = apt.patient_birth_date;
  if (doctorListId) fields['UF_CRM_1770366173566'] = doctorListId;

  if (includePlanFields && planData?.found) {
    const mapped = mapPlanToFields(planData.title, planData.direction);
    if (mapped.direction) { const id = await findDirectionId(ctx, mapped.direction); if (id) fields['UF_CRM_1770364332145'] = id; }
    if (mapped.indication) { const id = await findIndicationId(ctx, mapped.indication); if (id) fields['UF_CRM_1770364702542'] = id; }
  }
  return fields;
}

// =====================================================================
//  Хелперы
// =====================================================================

function parseDateDMY(str) { const [d, m, y] = str.split('.'); return new Date(parseInt(y), parseInt(m) - 1, parseInt(d)); }
function formatDateDMY(date) { return `${String(date.getDate()).padStart(2, '0')}.${String(date.getMonth() + 1).padStart(2, '0')}.${date.getFullYear()}`; }
function addDays(date, days) { const r = new Date(date); r.setDate(r.getDate() + days); return r; }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
