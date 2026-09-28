const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');

/**
 * Loads order rows from a CSV file.
 * Expected columns: customerid,productid,quantity,shopid,delay_seconds
 *
 * @param {string} [filePath]
 * @returns {Array<{customerid: string, productid: string, quantity: number, shopid: string, delay_seconds: number}>}
 */
function loadOrdersFromCsv(filePath) {
  const targetPath = filePath || path.resolve(__dirname, '../../data/orders.csv');
  const fileContent = fs.readFileSync(targetPath, 'utf-8');

  const records = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  return records.map(row => ({
    customerid: String(row.customerid),
    productid: String(row.productid),
    quantity: parseInt(row.quantity, 10),
    shopid: String(row.shopid),
    delay_seconds: parseFloat(row.delay_seconds || '0')
  }));
}

/**
 * Helper to execute a callback for each row respecting relative delay_seconds.
 *
 * @param {Array<{customerid: string, productid: string, quantity: number, shopid: string, delay_seconds: number}>} orders
 * @param {(order: object, index: number) => Promise<void>} onSendOrder
 */
async function scheduleOrders(orders, onSendOrder) {
  const startTime = Date.now();
  let previousDelay = 0;

  for (let i = 0; i < orders.length; i++) {
    const order = orders[i];
    const waitTimeMs = Math.max(0, (order.delay_seconds - previousDelay) * 1000);

    if (waitTimeMs > 0) {
      await new Promise(resolve => setTimeout(resolve, waitTimeMs));
    }

    const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(2);
    await onSendOrder(order, i, elapsedSeconds);
    previousDelay = order.delay_seconds;
  }
}

module.exports = {
  loadOrdersFromCsv,
  scheduleOrders
};
