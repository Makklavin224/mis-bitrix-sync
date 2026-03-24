#!/usr/bin/env node
import 'dotenv/config';

/**
 * Быстрая проверка товаров в сделках.
 * Только Битрикс, без МИС — работает быстро.
 *
 * Проверяет сделки в стадиях "Не пришёл на лечение" и "Нет показаний к лечению"
 * на наличие товаров завершения лечения.
 *
 * node check-products.js              — отчёт
 * DRY_RUN=0 node check-products.js    — переместить в "Лечение завершено"
 */

const BITRIX_URL = process.env.BITRIX_WEBHOOK_URL;
const DRY_RUN = process.env.DRY_RUN !== '0';

if (!BITRIX_URL) {
  console.error('Нужна переменная BITRIX_WEBHOOK_URL');
  process.exit(1);
}

const CHECK_STAGES = ['UC_QAU8BB', 'UC_LVYHC1']; // не пришёл на лечение, нет показаний
const TARGET_STAGE = 'UC_F92MOY'; // лечение завершено

const STAGE_NAMES = {
  'UC_QAU8BB': 'Не пришёл на лечение',
  'UC_LVYHC1': 'Нет показаний к лечению',
  'UC_F92MOY': 'Лечение завершено',
};

const TREATMENT_COMPLETE_KEYWORDS = [
  'повторный прием 2-й день',
  'повторный прием на 2-ой день',
  'повторный прием 2 день',
  'повторный прием 7 день',
  'повторный прием на 7-ой день',
  'повторный прием 14 день',
  'повторный прием на 21-й день',
  'повторный прием на 45-й день',
  'контрольный осмотр после лечения',
  'осмотр врача м',
  'снятие швов после операции',
  'перевязка',
];

console.log(`\n${'='.repeat(55)}`);
console.log(`  Проверка товаров → Лечение завершено`);
console.log(`  Режим: ${DRY_RUN ? 'ОТЧЁТ' : 'ИСПРАВЛЕНИЕ'}`);
console.log(`${'='.repeat(55)}\n`);

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

async function main() {
  // 1. Загрузить сделки из двух стадий
  console.log('[1/2] Загрузка сделок...');
  let allDeals = [];

  for (const stage of CHECK_STAGES) {
    let start = 0;
    while (true) {
      const res = await bitrix('crm.deal.list', {
        filter: { STAGE_ID: stage, CLOSED: 'N' },
        select: ['ID', 'TITLE', 'STAGE_ID', 'OPPORTUNITY', 'UF_CRM_1774345475'],
        start,
      });
      const deals = res.result || [];
      allDeals.push(...deals);
      if (!res.next) break;
      start = res.next;
    }
  }

  console.log(`  ${STAGE_NAMES[CHECK_STAGES[0]]}: ${allDeals.filter(d => d.STAGE_ID === CHECK_STAGES[0]).length}`);
  console.log(`  ${STAGE_NAMES[CHECK_STAGES[1]]}: ${allDeals.filter(d => d.STAGE_ID === CHECK_STAGES[1]).length}`);
  console.log(`  Всего: ${allDeals.length}\n`);

  // 2. Проверить товары каждой сделки
  console.log('[2/2] Проверка товаров...\n');

  const issues = [];
  let checked = 0;

  for (const deal of allDeals) {
    checked++;
    if (checked % 20 === 0) {
      process.stdout.write(`\r  Проверено: ${checked}/${allDeals.length}...`);
    }

    const res = await bitrix('crm.deal.productrows.get', { id: deal.ID });
    const products = res?.result || [];

    for (const p of products) {
      const title = (p.PRODUCT_NAME || '').toLowerCase();
      if (TREATMENT_COMPLETE_KEYWORDS.some(kw => title.includes(kw))) {
        issues.push({
          dealId: deal.ID,
          title: deal.TITLE,
          doctor: deal.UF_CRM_1774345475 || '?',
          amount: parseFloat(deal.OPPORTUNITY) || 0,
          stage: deal.STAGE_ID,
          product: p.PRODUCT_NAME,
        });
        break;
      }
    }
  }

  console.log(`\r  Проверено: ${checked}/${allDeals.length}      \n`);

  if (!issues.length) {
    console.log('  Не найдено сделок с товарами завершения лечения.\n');
    return;
  }

  // Группировка по стадии
  for (const stage of CHECK_STAGES) {
    const stageIssues = issues.filter(i => i.stage === stage);
    if (!stageIssues.length) continue;

    const sum = stageIssues.reduce((s, i) => s + i.amount, 0);
    console.log(`  ${STAGE_NAMES[stage]} → Лечение завершено`);
    console.log(`  Сделок: ${stageIssues.length}, сумма: ${sum.toLocaleString('ru-RU')} ₽\n`);

    for (const issue of stageIssues) {
      console.log(`    #${issue.dealId} | ${issue.doctor} | ${issue.amount.toLocaleString('ru-RU')} ₽ | товар: "${issue.product}"`);
    }
    console.log('');

    if (!DRY_RUN) {
      console.log(`  Перемещаю ${stageIssues.length} сделок...`);
      for (const issue of stageIssues) {
        await bitrix('crm.deal.update', {
          id: issue.dealId,
          fields: { STAGE_ID: TARGET_STAGE },
        });
      }
      console.log('  Готово.\n');
    }
  }

  const totalSum = issues.reduce((s, i) => s + i.amount, 0);
  console.log(`${'='.repeat(55)}`);
  console.log(`  Найдено: ${issues.length} сделок на ${totalSum.toLocaleString('ru-RU')} ₽`);
  if (DRY_RUN) {
    console.log(`  Для исправления: DRY_RUN=0 node check-products.js`);
  } else {
    console.log(`  Перемещено: ${issues.length} сделок`);
  }
  console.log('');
}

main().catch(err => {
  console.error('\nОшибка:', err.message);
  process.exit(1);
});
