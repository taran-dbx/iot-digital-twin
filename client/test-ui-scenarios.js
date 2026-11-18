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
    // Set localStorage theme
    localStorage.setItem('iot-twin-theme', t);
    // Set the data-theme attribute on root
    const root = document.documentElement;
    root.setAttribute('data-theme', t);
    root.classList.remove('light', 'dark');
    root.classList.add(t);
  }, theme);

  // Reload page to apply theme
  await page.reload({ waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(800); // Wait for theme to apply
}

async function setDevRole(page, role) {
  console.log(`  Setting dev role to: ${role}`);
  await page.evaluate((r) => {
    localStorage.setItem('devRoleOverride', r);
  }, role);

  // Reload to apply
  await page.reload({ waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(800);
}

async function setDevHealth(page, health) {
  console.log(`  Setting dev health to: ${health}`);
  await page.evaluate((h) => {
    localStorage.setItem('devMockHealthy', h === 'Healthy' ? 'true' : 'false');
  }, health);

  // Reload to apply
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

async function getIndustryCardId(page) {
  // Get the first industry card's id
  const id = await page.evaluate(() => {
    const card = document.querySelector('[data-industry-id]');
    return card ? card.getAttribute('data-industry-id') : null;
  });
  return id;
}

async function clickIndustryCard(page) {
  const initialUrl = page.url();

  await page.evaluate(() => {
    const cards = document.querySelectorAll('[data-testid="industry-card"]');
    if (cards.length > 0) {
      cards[0].click();
    }
  });

  // Wait for URL to change or page to load new content
  let attempts = 0;
  while (attempts < 30) {
    await sleep(200);
    const currentUrl = page.url();
    if (currentUrl !== initialUrl) {
      console.log(`  URL changed to: ${currentUrl}`);
      await sleep(1000); // Wait for content to render
      break;
    }
    attempts++;
  }
}

async function getConsoleErrors(page) {
  const errors = [];
  const listener = msg => {
    if (msg.type() === 'error') {
      errors.push(msg.text());
    }
  };
  page.on('console', listener);
  return { errors, listener };
}

async function captureConsoleErrors(page) {
  const errors = [];
  const messages = await page.evaluate(() => {
    // This won't capture actual console.error calls, but we can check network errors
    return {
      title: document.title,
      url: window.location.href
    };
  });
  return errors;
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

    // Set console error logging
    page.on('console', msg => {
      if (msg.type() === 'error') {
        console.warn(`  [CONSOLE ERROR] ${msg.text()}`);
      }
    });

    // Set viewport for full-page capture
    await page.setViewport({ width: 1440, height: 900 });

    console.log('\n=== TEST SEQUENCE: Home Page Admin View ===\n');

    // 1. Navigate to home
    console.log('Step 1: Navigate to http://localhost:3000/');
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(1000);

    // 1a. Set Admin role
    await setDevRole(page, 'Admin');
    screenshots.push(await takeScreenshot(page, 'home-admin-light.png'));

    // 1b. Switch to dark and capture
    console.log('  Switching to dark theme...');
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'home-admin-dark.png'));

    // 1c. Switch back to light
    console.log('  Switching to light theme...');
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: Home Page User View ===\n');

    // 2. Set User role
    await setDevRole(page, 'User');
    screenshots.push(await takeScreenshot(page, 'home-user-light.png'));

    // 2b. Dark theme
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'home-user-dark.png'));
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: User + Healthy Verdict ===\n');

    // 3. Set Healthy
    await setDevHealth(page, 'Healthy');

    // Get industry card ID and click it
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(800);

    console.log('  Clicking first industry card...');
    await clickIndustryCard(page);

    screenshots.push(await takeScreenshot(page, 'user-open-healthy-light.png'));

    // 3b. Dark theme
    await setTheme(page, 'dark');
    screenshots.push(await takeScreenshot(page, 'user-open-healthy-dark.png'));
    await setTheme(page, 'light');

    console.log('\n=== TEST SEQUENCE: User + Unhealthy Verdict ===\n');

    // 4. Set Unhealthy
    await setDevHealth(page, 'Unhealthy');

    // Reload the current page to apply unhealthy state
    await page.reload({ waitUntil: 'networkidle2', timeout: 60000 });
    await sleep(800);

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
