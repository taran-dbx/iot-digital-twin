import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_URL = 'http://localhost:3001';
const SCREENSHOT_DIR = '/Users/taranpreetsingh.grover/Documents/Claude Projects/Apps/iot-digital-twin/mockups/_ui-ref/audit';

// Ensure output directory exists
if (!fs.existsSync(SCREENSHOT_DIR)) {
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });
}

const routes = [
  {
    route: '/',
    filename: 'home-fixed.png',
    description: 'Home page - check for [dev] toggle removal, All pipelines button'
  },
  {
    route: '/pipelines',
    filename: 'pipelines-fixed.png',
    description: 'Pipelines list'
  },
  {
    route: '/setup-validate',
    filename: 'setup-validate-fixed.png',
    description: 'Setup validation - check Manufacturing/Energy are greyed with "already configured · edit"'
  },
  {
    route: '/i/pharmaceutical',
    filename: 'industry-setup-fixed.png',
    description: 'Pharmaceutical industry setup'
  },
  {
    route: '/i/pharmaceutical/realtime/dashboard',
    filename: 'mode-dashboard-fixed.png',
    description: 'Pharmaceutical realtime mode dashboard'
  }
];

async function captureScreenshots() {
  let browser;
  const results = [];
  const consoleErrors = {};

  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();

    // Set viewport to 1440px width
    await page.setViewport({ width: 1440, height: 1080 });

    // Collect all console messages
    page.on('console', msg => {
      const level = msg.type();
      const text = msg.text();

      if (!consoleErrors[level]) {
        consoleErrors[level] = [];
      }
      consoleErrors[level].push(text);
    });

    for (const route of routes) {
      console.log(`\nCapturing: ${route.filename}`);
      console.log(`  Route: ${route.route}`);

      // Navigate to route
      const url = `${BASE_URL}${route.route}`;
      try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 10000 });
      } catch (e) {
        console.log(`  Warning: Navigation timeout (app may still be rendering)`);
      }

      // Wait for 2 seconds for render
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Take full-page screenshot
      const outputPath = path.join(SCREENSHOT_DIR, route.filename);
      await page.screenshot({
        path: outputPath,
        fullPage: true
      });

      console.log(`  ✓ Saved to: ${outputPath}`);

      // Verify file exists
      if (fs.existsSync(outputPath)) {
        const stats = fs.statSync(outputPath);
        console.log(`  ✓ File verified (${stats.size} bytes)`);
      }

      results.push({
        filename: route.filename,
        fullPath: outputPath,
        description: route.description,
        route: route.route
      });
    }

    // Check for [dev] toggle on home page
    console.log('\n\nChecking for [dev] toggle on home page...');
    await page.goto(`${BASE_URL}/`, { waitUntil: 'networkidle2', timeout: 10000 });
    await new Promise(resolve => setTimeout(resolve, 1000));

    const devToggleExists = await page.evaluate(() => {
      // Look for any element with [dev] in text
      const allElements = document.querySelectorAll('*');
      for (let el of allElements) {
        if (el.textContent && el.textContent.includes('[dev]')) {
          return true;
        }
      }
      return false;
    });

    console.log(`  [dev] toggle visible: ${devToggleExists ? 'YES (BAD)' : 'NO (GOOD)'}`);

    // Check for "All pipelines" button on home page
    const allPipelinesExists = await page.evaluate(() => {
      const buttons = document.querySelectorAll('button');
      for (let btn of buttons) {
        if (btn.textContent.includes('All pipelines')) {
          return true;
        }
      }
      return false;
    });

    console.log(`  "All pipelines" button visible: ${allPipelinesExists ? 'YES (admin detected)' : 'NO (non-admin user, expected)'}`);

    // Check setup-validate for greyed industries
    console.log('\nChecking setup-validate for greyed industries...');
    await page.goto(`${BASE_URL}/setup-validate`, { waitUntil: 'networkidle2', timeout: 10000 });
    await new Promise(resolve => setTimeout(resolve, 1000));

    const greyedInfo = await page.evaluate(() => {
      const result = {
        manufacturing: { greyed: false, hasLabel: false },
        energy: { greyed: false, hasLabel: false },
        other: { count: 0 }
      };

      // Find all industry cards/buttons
      const industryElements = document.querySelectorAll('[class*="industry"], [class*="card"], button');

      for (let el of industryElements) {
        const text = el.textContent.toLowerCase();
        const style = window.getComputedStyle(el);
        const opacity = style.opacity;
        const pointerEvents = style.pointerEvents;

        if (text.includes('manufacturing')) {
          result.manufacturing.greyed = opacity < 1 || pointerEvents === 'none';
          result.manufacturing.hasLabel = text.includes('already configured');
        } else if (text.includes('energy')) {
          result.energy.greyed = opacity < 1 || pointerEvents === 'none';
          result.energy.hasLabel = text.includes('already configured');
        } else if (text.includes('pharma')) {
          result.other.count++;
        }
      }

      return result;
    });

    console.log(`  Manufacturing greyed: ${greyedInfo.manufacturing.greyed ? 'YES' : 'NO'}`);
    console.log(`  Manufacturing has "already configured" label: ${greyedInfo.manufacturing.hasLabel ? 'YES' : 'NO'}`);
    console.log(`  Energy greyed: ${greyedInfo.energy.greyed ? 'YES' : 'NO'}`);
    console.log(`  Energy has "already configured" label: ${greyedInfo.energy.hasLabel ? 'YES' : 'NO'}`);

    await browser.close();

    // Print summary
    console.log('\n' + '='.repeat(80));
    console.log('AFTER-STATE SCREENSHOT CAPTURE SUMMARY');
    console.log('='.repeat(80));

    results.forEach(result => {
      console.log(`\n${result.filename}`);
      console.log(`  Full Path: ${result.fullPath}`);
      console.log(`  Route: ${result.route}`);
      console.log(`  Description: ${result.description}`);
    });

    console.log('\n' + '='.repeat(80));
    console.log('KEY FINDINGS:');
    console.log(`  [dev] toggle removed: ${!devToggleExists ? 'YES ✓' : 'NO ✗'}`);
    console.log(`  "All pipelines" button: ${allPipelinesExists ? 'VISIBLE (admin)' : 'NOT VISIBLE (non-admin, expected)'}`);
    console.log(`  Manufacturing greyed + label: ${greyedInfo.manufacturing.greyed && greyedInfo.manufacturing.hasLabel ? 'YES ✓' : 'NO ✗'}`);
    console.log(`  Energy greyed + label: ${greyedInfo.energy.greyed && greyedInfo.energy.hasLabel ? 'YES ✓' : 'NO ✗'}`);
    console.log('\nConsole errors/warnings:');
    if (Object.keys(consoleErrors).length === 0) {
      console.log('  None detected');
    } else {
      Object.entries(consoleErrors).forEach(([level, messages]) => {
        console.log(`  ${level.toUpperCase()}: ${messages.length} messages`);
        messages.slice(0, 3).forEach(msg => console.log(`    - ${msg}`));
        if (messages.length > 3) {
          console.log(`    ... and ${messages.length - 3} more`);
        }
      });
    }
    console.log('\n' + '='.repeat(80));
    console.log(`All ${results.length} AFTER-state screenshots captured successfully!`);
    console.log(`Saved to: ${SCREENSHOT_DIR}`);
    console.log('='.repeat(80));

  } catch (error) {
    console.error('Error during screenshot capture:', error);
    process.exit(1);
  }
}

captureScreenshots();
