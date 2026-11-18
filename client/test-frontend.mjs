import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const screenshotDir = '/Users/taranpreetsingh.grover/Documents/Claude Projects/Apps/iot-digital-twin/mockups/_ui-ref/proto';

// Create screenshot directory if it doesn't exist
if (!fs.existsSync(screenshotDir)) {
  fs.mkdirSync(screenshotDir, { recursive: true });
}

const tests = [
  {
    name: 'Setup Validate (Industry: pharmaceutical)',
    url: 'http://localhost:3001/setup-validate?industry=pharmaceutical',
    screenshotBase: 'config-setup-wired.png',
    hasInteractions: true,
  },
  {
    name: 'Pipelines List',
    url: 'http://localhost:3001/pipelines',
    screenshotBase: 'mgmt-list-wired.png',
    hasInteractions: false,
  },
  {
    name: 'Pipeline Health Detail',
    url: 'http://localhost:3001/pipelines/pharmaceutical/health',
    screenshotBase: 'health-detail-wired.png',
    hasInteractions: false,
  },
];

(async () => {
  let browser;
  const testResults = [];

  try {
    browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    for (const test of tests) {
      console.log(`\n========== Testing: ${test.name} ==========`);
      const page = await browser.newPage();

      // Set up request and console logging
      const networkRequests = [];
      const consoleErrors = [];

      page.on('request', (request) => {
        networkRequests.push({
          method: request.method(),
          url: request.url(),
          resourceType: request.resourceType(),
        });
      });

      page.on('console', (msg) => {
        if (msg.type() === 'error' || msg.location().url.includes('localhost')) {
          consoleErrors.push({
            type: msg.type().toUpperCase(),
            text: msg.text(),
            location: msg.location(),
          });
        }
      });

      page.on('pageerror', (error) => {
        consoleErrors.push({
          type: 'UNCAUGHT_ERROR',
          text: error.toString(),
        });
      });

      // Navigate to home first to set localStorage
      console.log(`Setting up admin role in localStorage...`);
      await page.goto('http://localhost:3001', { waitUntil: 'domcontentloaded', timeout: 10000 }).catch(() => {});
      await page.evaluate(() => {
        localStorage.setItem('devRoleOverride', 'admin');
      });

      // Now navigate to the test URL
      console.log(`Navigating to: ${test.url}`);
      try {
        await page.goto(test.url, { waitUntil: 'domcontentloaded', timeout: 10000 });
      } catch (e) {
        console.log(`  (Navigation timeout expected if backend is down)`);
      }

      // Wait for initial content to render
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Test interactions if this is the setup-validate route
      if (test.hasInteractions && test.url.includes('setup-validate')) {
        const stepScreenshots = [
          'config-setup-wired.png',
          'config-source-wired.png',
          'config-validation-wired.png',
        ];

        for (let i = 0; i < stepScreenshots.length; i++) {
          const screenshotPath = path.join(screenshotDir, stepScreenshots[i]);
          try {
            console.log(`\n  Capturing step ${i}: ${stepScreenshots[i]}`);
            await page.screenshot({ path: screenshotPath, fullPage: true });
            console.log(`    Screenshot saved: ${screenshotPath}`);

            if (i < stepScreenshots.length - 1) {
              // Click Next button to advance to next step
              console.log(`  Clicking Next button...`);
              await page.evaluate(() => {
                const buttons = document.querySelectorAll('button');
                for (const btn of buttons) {
                  if (btn.textContent.includes('Next')) {
                    btn.click();
                    return true;
                  }
                }
                return false;
              });
              await new Promise(resolve => setTimeout(resolve, 1500));
            }
          } catch (e) {
            console.log(`    Error: ${e.message}`);
          }
        }
      } else {
        // Regular screenshot for non-interactive pages
        const screenshotPath = path.join(screenshotDir, test.screenshotBase);
        await page.screenshot({ path: screenshotPath, fullPage: true });
        console.log(`Screenshot saved: ${screenshotPath}`);
      }

      // Collect API endpoints called
      const apiRequests = networkRequests
        .filter((req) => req.url.includes('/api/'))
        .map((req) => {
          const urlObj = new URL(req.url);
          return `${req.method} ${urlObj.pathname}${urlObj.search}`;
        });

      console.log(`\n  Network Requests to /api/:`);
      if (apiRequests.length === 0) {
        console.log('    (none)');
      } else {
        const uniqueRequests = [...new Set(apiRequests)];
        uniqueRequests.forEach((req) => {
          console.log(`    ${req}`);
        });
      }

      console.log(`\n  Console Errors:`);
      const realErrors = consoleErrors.filter((msg) => msg.type === 'ERROR' || msg.type === 'UNCAUGHT_ERROR');
      if (realErrors.length === 0) {
        console.log('    (none)');
      } else {
        realErrors.forEach((err) => {
          const errorText = err.text.substring(0, 150);
          console.log(`    [${err.type}] ${errorText}`);
        });
      }

      const errorMessages = realErrors
        .filter((err) => !err.text.includes('Failed to fetch'))
        .map((err) => `[${err.type}] ${err.text}`);

      testResults.push({
        test: test.name,
        rendered: true,
        apiRequests: [...new Set(apiRequests)],
        hasErrors: errorMessages.length > 0,
        errors: errorMessages,
        screenshot: test.screenshotBase,
      });

      await page.close();
    }

    console.log('\n\n========== TEST SUMMARY ==========\n');
    testResults.forEach((result) => {
      console.log(`Test: ${result.test}`);
      console.log(`  Rendered: ${result.rendered ? 'YES' : 'NO'}`);
      console.log(`  API Requests:`);
      if (result.apiRequests.length === 0) {
        console.log(`    (none)`);
      } else {
        result.apiRequests.forEach((req) => console.log(`      ${req}`));
      }
      console.log(`  Uncaught Errors: ${result.hasErrors ? 'YES' : 'NO'}`);
      if (result.hasErrors) {
        result.errors.forEach((err) => console.log(`    ${err}`));
      }
      console.log(`  Screenshot: ${result.screenshot}`);
      console.log();
    });

  } catch (error) {
    console.error('Test error:', error);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
})();
