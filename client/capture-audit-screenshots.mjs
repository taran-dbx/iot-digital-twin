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
    devToggle: 'User',
    filename: 'home-user-before.png',
    description: 'Home page as User'
  },
  {
    route: '/',
    devToggle: 'Admin',
    filename: 'home-admin-before.png',
    description: 'Home page as Admin'
  },
  {
    route: '/pipelines',
    devToggle: null,
    filename: 'pipelines-before.png',
    description: 'Pipelines list'
  },
  {
    route: '/setup-validate',
    devToggle: null,
    filename: 'setup-validate-before.png',
    description: 'Setup validation (step 1: Flavor & Industry grid)'
  },
  {
    route: '/i/pharmaceutical',
    devToggle: null,
    filename: 'industry-setup-before.png',
    description: 'Pharmaceutical industry setup'
  },
  {
    route: '/i/pharmaceutical/realtime/dashboard',
    devToggle: null,
    filename: 'mode-dashboard-before.png',
    description: 'Pharmaceutical realtime mode dashboard'
  }
];

async function captureScreenshots() {
  let browser;
  const results = [];
  const consoleMessages = {};

  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();

    // Set viewport to 1440px width
    await page.setViewport({ width: 1440, height: 1080 });

    for (const route of routes) {
      console.log(`\nCapturing: ${route.filename}`);
      console.log(`  Route: ${route.route}${route.devToggle ? ` [dev toggle: ${route.devToggle}]` : ''}`);

      // Clear console messages for this route
      consoleMessages[route.filename] = [];

      // Set up console message listener
      page.on('console', msg => {
        const level = msg.type();
        if (level === 'error' || level === 'warning') {
          consoleMessages[route.filename].push(`${level.toUpperCase()}: ${msg.text()}`);
        }
      });

      // Navigate to route
      const url = `${BASE_URL}${route.route}`;
      try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 10000 });
      } catch (e) {
        console.log(`  Warning: Navigation timeout (app may still be rendering)`);
      }

      // Wait for 2 seconds for rendering
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Handle dev toggle if needed
      if (route.devToggle) {
        console.log(`  Setting dev toggle to: ${route.devToggle}`);

        try {
          // Find button with the toggle text in topbar
          const toggleButton = await page.evaluateHandle((text) => {
            const buttons = document.querySelectorAll('button');
            for (let btn of buttons) {
              if (btn.textContent.trim() === text) {
                return btn;
              }
            }
            return null;
          }, route.devToggle);

          const isNull = await page.evaluate(btn => btn === null, toggleButton);
          
          if (!isNull) {
            await toggleButton.click();
            await new Promise(resolve => setTimeout(resolve, 1000));
            console.log(`  ✓ Dev toggle set`);
          } else {
            console.log(`  Warning: Could not find ${route.devToggle} toggle button`);
          }
        } catch (e) {
          console.log(`  Warning: Error setting dev toggle: ${e.message}`);
        }
      }

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
        devToggle: route.devToggle,
        route: route.route,
        consoleErrors: consoleMessages[route.filename]
      });

      // Remove the listener for next iteration
      page.removeAllListeners('console');
    }

    await browser.close();

    // Print summary
    console.log('\n' + '='.repeat(80));
    console.log('SCREENSHOT CAPTURE SUMMARY');
    console.log('='.repeat(80));

    results.forEach(result => {
      console.log(`\n${result.filename}`);
      console.log(`  Full Path: ${result.fullPath}`);
      console.log(`  Route: ${result.route}${result.devToggle ? ` (dev toggle: ${result.devToggle})` : ''}`);
      console.log(`  Description: ${result.description}`);
      if (result.consoleErrors && result.consoleErrors.length > 0) {
        console.log(`  Console Issues: ${result.consoleErrors.join(' | ')}`);
      } else {
        console.log(`  Console: OK (no errors/warnings)`);
      }
    });

    console.log('\n' + '='.repeat(80));
    console.log(`All ${results.length} screenshots captured successfully!`);
    console.log('='.repeat(80));

  } catch (error) {
    console.error('Error during screenshot capture:', error);
    process.exit(1);
  }
}

captureScreenshots();
