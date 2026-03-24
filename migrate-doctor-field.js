#!/usr/bin/env node
import 'dotenv/config';

/**
 * Миграция поля врача: старый список UF_CRM_1770366173566 → новый текст UF_CRM_1774345475
 *
 * Проходит по всем открытым сделкам, читает ID из старого списочного поля,
 * находит ФИО врача по этому ID, записывает в новое текстовое поле.
 *
 * Запуск:
 *   node migrate-doctor-field.js              — dry run (только показать)
 *   DRY_RUN=0 node migrate-doctor-field.js    — записать
 */

const BITRIX_URL = process.env.BITRIX_WEBHOOK_URL;
const DRY_RUN = process.env.DRY_RUN !== '0';

if (!BITRIX_URL) {
  console.error('Нужна переменная BITRIX_WEBHOOK_URL');
  process.exit(1);
}

console.log(`\n${'='.repeat(50)}`);
console.log(`  Миграция поля врача: список → текст`);
console.log(`  Режим: ${DRY_RUN ? 'DRY RUN (только показать)' : 'БОЕВОЙ (будет записывать)'}`);
console.log(`${'='.repeat(50)}\n`);

async function bitrix(method, params) {
  const url = `${BITRIX_URL.replace(/\/$/, '')}/${method}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  const data = await res.json();
  if (data.error) throw new Error(`Bitrix ${method}: ${data.error} — ${data.error_description}`);
  return data;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  // 1. Получить список врачей из старого списочного поля
  console.log('[1/3] Загрузка списка врачей из Битрикса...');
  const fieldsRes = await bitrix('crm.deal.fields', {});
  const doctorField = fieldsRes?.result?.['UF_CRM_1770366173566'];

  if (!doctorField?.items?.length) {
    console.log('  Старое поле врача не найдено или пустое. Нечего мигрировать.');
    return;
  }

  // ID → ФИО
  const idToName = new Map();
  for (const item of doctorField.items) {
    idToName.set(String(item.ID), item.VALUE?.trim());
  }
  console.log(`  Найдено ${idToName.size} врачей в списке:\n`);
  for (const [id, name] of idToName) {
    console.log(`    ${id} → ${name}`);
  }
  console.log('');

  // 2. Загрузить все открытые сделки
  console.log('[2/3] Загрузка открытых сделок...');
  let allDeals = [];
  let start = 0;

  while (true) {
    const res = await bitrix('crm.deal.list', {
      filter: { CLOSED: 'N' },
      select: ['ID', 'TITLE', 'UF_CRM_1770366173566', 'UF_CRM_1774345475'],
      start,
    });
    const deals = res.result || [];
    allDeals.push(...deals);
    console.log(`  Загружено: ${allDeals.length}...`);
    if (!res.next) break;
    start = res.next;
    await sleep(200);
  }
  console.log(`  Всего открытых сделок: ${allDeals.length}\n`);

  // 3. Мигрировать
  console.log('[3/3] Миграция...\n');

  let migrated = 0;
  let skipped = 0;
  let noOldField = 0;
  let alreadyFilled = 0;

  for (const deal of allDeals) {
    const oldValue = deal.UF_CRM_1770366173566;
    const newValue = deal.UF_CRM_1774345475;

    // Новое поле уже заполнено — пропускаем
    if (newValue && newValue.trim()) {
      alreadyFilled++;
      continue;
    }

    // Старое поле пустое — нечего мигрировать
    if (!oldValue) {
      noOldField++;
      continue;
    }

    const doctorName = idToName.get(String(oldValue));
    if (!doctorName) {
      console.log(`  #${deal.ID} "${deal.TITLE}" — старое поле=${oldValue}, врач не найден в списке`);
      skipped++;
      continue;
    }

    console.log(`  #${deal.ID} "${deal.TITLE}" — ${oldValue} → "${doctorName}"`);
    migrated++;

    if (!DRY_RUN) {
      await bitrix('crm.deal.update', {
        id: deal.ID,
        fields: { 'UF_CRM_1774345475': doctorName },
      });
      await sleep(300);
    }
  }

  console.log(`\n${'='.repeat(50)}`);
  console.log(`  ИТОГ`);
  console.log(`${'='.repeat(50)}`);
  console.log(`  Всего сделок:           ${allDeals.length}`);
  console.log(`  Мигрировано:            ${migrated}`);
  console.log(`  Уже заполнено (новое):  ${alreadyFilled}`);
  console.log(`  Нет старого поля:       ${noOldField}`);
  console.log(`  Пропущено (не найден):  ${skipped}`);
  if (DRY_RUN) {
    console.log(`\n  Режим DRY RUN — ничего не записано`);
    console.log(`  Для записи: DRY_RUN=0 node migrate-doctor-field.js`);
  }
  console.log('');
}

main().catch(err => {
  console.error('\nОшибка:', err.message);
  process.exit(1);
});
