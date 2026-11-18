import puppeteer from 'puppeteer';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const screenshotDir = '/Users/taranpreetsingh.grover/Documents/Claude Projects/Apps/iot-digital-twin/mockups/_ui-ref/audit';

// Create screenshot directory if it doesn't exist
if (!fs.existsSync(screenshotDir)) {
  fs.mkdirSync(screenshotDir, { recursive: true });
}

const screenshotPath = path.join(screenshotDir, 'home-deployed-only-v3.png');

async function testHomeEmptyState() {
  let browser;
  const testResults = {
    emptyStateRendered: false,
    configureButtonVisible: false,
    allPipelinesButtonVisible: false,
    industryCardsPresent: false,
    consoleErrors: [],
    networkErrors: [],
    screenshotPath: screenshotPath
  };

  try {
    console.log('Launching browser...');
    browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1920, height: 1080 });

    // Set up console and network logging
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        testResults.consoleErrors.push({
          type: msg.type().toUpperCase(),
          text: msg.text(),
        });
        console.log(`[CONSOLE ERROR] ${msg.text()}`);
      }
    });

    page.on('response', (response) => {
      if (response.status() >= 400) {
        testResults.networkErrors.push({
          status: response.status(),
          url: response.url(),
        });
        console.log(`[NETWORK ERROR] ${response.status()} - ${response.url()}`);
      }
    });

    page.on('pageerror', (error) => {
      testResults.consoleErrors.push({
        type: 'UNCAUGHT_ERROR',
        text: error.toString(),
      });
      console.log(`[PAGE ERROR] ${error.toString()}`);
    });

    console.log('\nNavigating to http://localhost:4185/');
    try {
      await page.goto('http://localhost:4185/', {
        waitUntil: 'networkidle2',
        timeout: 10000
      });
    } catch (e) {
      console.log(`Navigation error: ${e.message}`);
    }

    // Wait for 3 seconds for the SPA to fully render and fetch API data
    console.log('Waiting 3 seconds for SPA to render and fetch API data...');
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Evaluate the page to check for empty state and other elements
    const pageContent = await page.evaluate(() => {
      const result = {
        hasEmptyStateHeading: false,
        emptyStateText: '',
        hasConfigureButton: false,
        hasAllPipelinesButton: false,
        industryCardsPresent: false,
        industryCardNames: [],
        allText: document.body.innerText.substring(0, 2000),
        elementCount: {
          buttons: document.querySelectorAll('button').length,
          cards: document.querySelectorAll('[class*="card"]').length,
        }
      };

      // Check for empty state heading
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      headings.forEach(heading => {
        const text = heading.textContent.trim();
        if (text.includes('No pipelines deployed yet')) {
          result.hasEmptyStateHeading = true;
          result.emptyStateText = text;
        }
      });

      // Check all text content for empty state message
      if (!result.hasEmptyStateHeading) {
        const allElements = document.querySelectorAll('*');
        for (const el of allElements) {
          const text = el.textContent;
          if (text && text.includes('No pipelines deployed yet')) {
            result.hasEmptyStateHeading = true;
            result.emptyStateText = 'Found in: ' + el.tagName;
            break;
          }
        }
      }

      // Check for buttons
      const allButtons = document.querySelectorAll('button, a, [role="button"]');
      allButtons.forEach(btn => {
        const text = btn.textContent.trim();
        if (text.includes('Configure a pipeline')) {
          result.hasConfigureButton = true;
        }
        if (text.includes('All pipelines')) {
          result.hasAllPipelinesButton = true;
        }
      });

      // Check for industry cards
      const potentialIndustryElements = [
        ...document.querySelectorAll('[class*="industry"]'),
        ...document.querySelectorAll('[class*="card"]'),
        ...document.querySelectorAll('div[role="button"]'),
      ];

      const industryNames = ['Automotive', 'Manufacturing', 'Energy', 'Pharmaceutical', 'Finance', 'Healthcare', 'Retail', 'Telecom'];
      const industrySet = new Set();

      potentialIndustryElements.forEach(el => {
        const text = el.textContent.toLowerCase();
        industryNames.forEach(name => {
          if (text.includes(name.toLowerCase())) {
            industrySet.add(name);
          }
        });
      });

      result.industryCardsPresent = industrySet.size > 0;
      result.industryCardNames = Array.from(industrySet);

      return result;
    });

    testResults.emptyStateRendered = pageContent.hasEmptyStateHeading;
    testResults.configureButtonVisible = pageContent.hasConfigureButton;
    testResults.allPipelinesButtonVisible = pageContent.hasAllPipelinesButton;
    testResults.industryCardsPresent = pageContent.industryCardsPresent;

    console.log('\n========== PAGE CONTENT ANALYSIS ==========');
    console.log(`Empty state heading found: ${pageContent.hasEmptyStateHeading ? 'YES' : 'NO'}`);
    if (pageContent.emptyStateText) console.log(`  Text: "${pageContent.emptyStateText}"`);
    console.log(`Configure button visible: ${pageContent.hasConfigureButton ? 'YES' : 'NO'}`);
    console.log(`All pipelines button visible: ${pageContent.hasAllPipelinesButton ? 'YES' : 'NO'}`);
    console.log(`Industry cards present: ${pageContent.industryCardsPresent ? 'YES (REGRESSION!)' : 'NO (CORRECT)'}`);
    if (pageContent.industryCardNames.length > 0) {
      console.log(`  Found: ${pageContent.industryCardNames.join(', ')}`);
    }
    console.log(`Button count: ${pageContent.elementCount.buttons}`);
    console.log(`Card-like elements: ${pageContent.elementCount.cards}`);

    // Take full-page screenshot
    console.log(`\nTaking full-page screenshot...`);
    await page.screenshot({
      path: screenshotPath,
      fullPage: true,
    });

    console.log(`Screenshot saved to: ${screenshotPath}`);

    await browser.close();

    // Print results
    console.log('\n========== TEST RESULTS ==========');
    console.log(`Empty state rendered: ${testResults.emptyStateRendered ? 'PASS' : 'FAIL'}`);
    console.log(`Configure button visible: ${testResults.configureButtonVisible ? 'PASS' : 'FAIL'}`);
    console.log(`All pipelines button visible: ${testResults.allPipelinesButtonVisible ? 'PASS' : 'FAIL'}`);
    console.log(`NO industry cards present: ${!testResults.industryCardsPresent ? 'PASS' : 'FAIL - REGRESSION DETECTED'}`);
    console.log(`Console errors: ${testResults.consoleErrors.length > 0 ? 'FOUND' : 'NONE'}`);
    console.log(`Network errors: ${testResults.networkErrors.length > 0 ? 'FOUND' : 'NONE'}`);

    if (testResults.consoleErrors.length > 0) {
      console.log('\nConsole Errors:');
      testResults.consoleErrors.forEach(err => {
        console.log(`  - [${err.type}] ${err.text}`);
      });
    }

    if (testResults.networkErrors.length > 0) {
      console.log('\nNetwork Errors:');
      testResults.networkErrors.forEach(err => {
        console.log(`  - ${err.status} ${err.url}`);
      });
    }

    console.log(`\nScreenshot path: ${screenshotPath}`);

    // Exit with error code if critical tests failed
    if (!testResults.emptyStateRendered || testResults.industryCardsPresent) {
      console.log('\n❌ CRITICAL ISSUES DETECTED');
      process.exit(1);
    } else {
      console.log('\n✓ All critical tests passed');
      process.exit(0);
    }

  } catch (error) {
    console.error('\nFatal error:', error.message);
    console.error(error.stack);
    if (browser) await browser.close();
    process.exit(1);
  }
}

testHomeEmptyState();
