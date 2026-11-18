import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const OUTPUT_DIR = '/Users/taranpreetsingh.grover/Documents/Claude Projects/Apps/iot-digital-twin/mockups/_ui-ref/proto';

// Ensure output directory exists
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function setTheme(page, theme) {
  await page.evaluate((t) => {
    localStorage.setItem('iot-twin-theme', t);
    const root = document.documentElement;
    root.setAttribute('data-theme', t);
    root.classList.remove('light', 'dark');
    root.classList.add(t);
  }, theme);

  await page.reload({ waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(800);
}

async function setDevRole(page, role) {
  console.log(`  Setting dev role to: ${role}`);
  await page.evaluate((r) => {
    localStorage.setItem('devRoleOverride', r);
  }, role);

  await page.reload({ waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(800);
}

async function setDevHealth(page, health) {
  console.log(`  Setting dev health to: ${health}`);
  await page.evaluate((h) => {
    localStorage.setItem('devMockHealthy', h === 'Healthy' ? 'true' : 'false');
  }, health);

  await page.reload({ waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(800);
}

async function takeScreenshot(page, filename) {
  const filepath = path.join(OUTPUT_DIR, filename);
  await page.screenshot({
    path: filepath,
    fullPage: true
  });
  console.log(`  ✓ Saved: ${filename}`);
  return filepath;
}

async function getFirstIndustryId(page) {
  const id = await page.evaluate(() => {
    const cards = document.querySelectorAll('[data-testid="industry-card"], a[href^="/i/"]');
    if (cards.length > 0) {
      const href = cards[0].getAttribute('href');
      if (href) {
        const match = href.match(/\/i\/([^/]+)/);
        if (match) return match[1];
      }
      const dataId = cards[0].getAttribute('data-industry-id');
      if (dataId) return dataId;
    }
    // Try to extract from any visible industry text
    return 'pharmaceutical'; // Default fallback
  });
  return id;
}

async function verifyAdminControls(page, shouldExist) {
  const controls = await page.evaluate(() => {
    // More flexible selectors - no :has-text() which is not standard CSS
    const allButtons = Array.from(document.querySelectorAll('button'));
    const hasConfig = allButtons.some(b => b.textContent.includes('Config') || b.getAttribute('aria-label')?.includes('Config'));
    const hasStatus = allButtons.some(b => b.textContent.includes('Status') || b.getAttribute('aria-label')?.includes('Status'));

    // Look for "Add industry pipeline" dashed card
    const addCard = Array.from(document.querySelectorAll('*')).some(el =>
      el.textContent.includes('Add industry pipeline') || el.textContent.includes('add') && el.textContent.includes('pipeline')
    );

    // Also look for any admin-only card footers
    const adminFooters = document.querySelectorAll('[data-admin-footer]');

    return {
      hasConfigBtn: hasConfig,
      hasStatusBtn: hasStatus,
      hasAddCard: addCard,
      adminFooterCount: adminFooters.length,
      allButtonTexts: allButtons.map(b => b.textContent.trim()).filter(t => t.length > 0 && t.length < 50)
    };
  });

  console.log(`  Admin controls verification (should exist: ${shouldExist}):`);
  console.log(`    - Has Config button: ${controls.hasConfigBtn}`);
  console.log(`    - Has Status button: ${controls.hasStatusBtn}`);
  console.log(`    - Has Add industry pipeline card: ${controls.hasAddCard}`);
  console.log(`    - Admin footer elements: ${controls.adminFooterCount}`);
  console.log(`    - Button texts found: ${controls.allButtonTexts.slice(0, 10).join(', ')}`);

  return controls;
}

async function verifyVerdicts(page, expectedHealth) {
  const verdicts = await page.evaluate(() => {
    // Look for verdict boxes on flavor cards
    const verdictBoxes = document.querySelectorAll('[data-testid*="verdict"], [class*="verdict"]');
    const verdictTexts = Array.from(verdictBoxes).map(v => v.textContent.trim());

    // Look for health status text
    const healthTexts = Array.from(document.querySelectorAll('*')).filter(el => {
      const text = el.textContent;
      return text && (text.includes('Healthy') || text.includes('healthy') || text.includes('ready to start'));
    }).map(el => el.textContent.trim().substring(0, 80));

    // Look for Start Pipeline buttons
    const startBtns = Array.from(document.querySelectorAll('button')).filter(b =>
      b.textContent.includes('Start') || b.textContent.includes('start')
    ).map(b => b.textContent.trim());

    return {
      verdictCount: verdictBoxes.length,
      verdictSamples: verdictTexts.slice(0, 3),
      healthStatusSamples: healthTexts.slice(0, 5),
      startButtonCount: startBtns.length,
      startButtonSamples: startBtns.slice(0, 3)
    };
  });

  console.log(`  Verdict verification (expected: ${expectedHealth}):`);
  console.log(`    - Verdict boxes found: ${verdicts.verdictCount}`);
  console.log(`    - Health status samples: ${verdicts.healthStatusSamples.join(' | ')}`);
  console.log(`    - Start buttons found: ${verdicts.startButtonCount}`);
  console.log(`    - Start button samples: ${verdicts.startButtonSamples.join(' | ')}`);

  return verdicts;
}

async function main() {
  let browser;
  const screenshots = [];

  try {
    browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();

    page.on('console', msg => {
      if (msg.type() === 'error') {
        console.warn(`  [CONSOLE ERROR] ${msg.text()}`);
      }
    });

    await page.setViewport({ width: 1440, height: 900 });

    console.log('\n=== TEST SEQUENCE: Home Page Admin View ===\n');

    // 1. Navigate to home and set Admin
    console.log('Step 1: Navigate to http://localhost:3000/ with Admin role');
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(1000);

    await setDevRole(page, 'Admin');

    console.log('  Verifying admin controls...');
    await verifyAdminControls(page, true);

    screenshots.push(await takeScreenshot(page, 'home-admin-light.png'));

    // 1b. Dark theme
    console.log('  Switching to dark theme...');
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'home-admin-dark.png'));
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: Home Page User View ===\n');

    // 2. Set User role
    await setDevRole(page, 'User');

    console.log('  Verifying admin controls are hidden...');
    await verifyAdminControls(page, false);

    screenshots.push(await takeScreenshot(page, 'home-user-light.png'));

    // 2b. Dark theme
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'home-user-dark.png'));
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: User + Healthy Verdict ===\n');

    // 3. Set Healthy and navigate to industry detail
    await setDevHealth(page, 'Healthy');

    // Get the first industry ID
    const industryId = await getFirstIndustryId(page);
    console.log(`  Using industry ID: ${industryId}`);

    // Navigate directly to industry setup page
    const industryUrl = `http://localhost:3000/i/${industryId}`;
    console.log(`  Navigating to: ${industryUrl}`);
    await page.goto(industryUrl, { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(1000);

    console.log('  Verifying healthy verdicts...');
    await verifyVerdicts(page, 'Healthy');

    screenshots.push(await takeScreenshot(page, 'user-open-healthy-light.png'));

    // 3b. Dark theme
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'user-open-healthy-dark.png'));
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: User + Unhealthy Verdict ===\n');

    // 4. Set Unhealthy
    await setDevHealth(page, 'Unhealthy');

    // Reload the detail page
    console.log(`  Reloading: ${industryUrl}`);
    await page.goto(industryUrl, { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(1000);

    console.log('  Verifying unhealthy verdicts...');
    await verifyVerdicts(page, 'Unhealthy');

    screenshots.push(await takeScreenshot(page, 'user-open-unhealthy-light.png'));

    // 4b. Dark theme
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'user-open-unhealthy-dark.png'));
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: Pipelines Management List ===\n');

    // 5. Navigate to /pipelines
    console.log('Step 5: Navigate to http://localhost:3000/pipelines');
    await page.goto('http://localhost:3000/pipelines', { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(1000);

    screenshots.push(await takeScreenshot(page, 'mgmt-list-light.png'));

    // 5b. Dark theme
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'mgmt-list-dark.png'));
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: Health Detail Page ===\n');

    // 6. Navigate to /pipelines/pharmaceutical/health
    console.log('Step 6: Navigate to http://localhost:3000/pipelines/pharmaceutical/health');
    await page.goto('http://localhost:3000/pipelines/pharmaceutical/health', { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(1000);

    screenshots.push(await takeScreenshot(page, 'health-detail-light.png'));

    // 6b. Dark theme
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'health-detail-dark.png'));

    console.log('\n✅ All screenshots captured successfully!');
    console.log(`\nOutput directory: ${OUTPUT_DIR}`);
    console.log(`\nScreenshots taken:`);
    screenshots.forEach((s, idx) => {
      const filename = path.basename(s);
      const size = (fs.statSync(s).size / 1024).toFixed(2);
      console.log(`  ${idx + 1}. ${filename} (${size} KB)`);
    });

  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

main();
