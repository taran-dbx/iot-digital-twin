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

const STEPS = [
  { num: 0, stepNum: 1, name: 'flavor', heading: 'Flavor & Industry' },
  { num: 1, stepNum: 2, name: 'source', heading: 'Source' },
  { num: 2, stepNum: 3, name: 'preview', heading: 'Setup Preview' },
  { num: 3, stepNum: 4, name: 'validation', heading: 'Validation' },
  { num: 4, stepNum: 5, name: 'metrics', heading: 'Metrics' },
  { num: 5, stepNum: 6, name: 'notebook', heading: 'Notebook' }
];

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function verifyStepperAndHeading(page, expectedStepNum, expectedHeading) {
  const info = await page.evaluate((stepNum, heading) => {
    // Find the filled (red) circle in the stepper
    const stepperCircles = document.querySelectorAll('[class*="rounded-full"][class*="bg-"]');
    let filledStepNum = null;

    // Look for the active step indicator - typically a filled circle with bg-red
    const activeCircle = document.querySelector('[class*="bg-red"]');
    if (activeCircle) {
      const circles = document.querySelectorAll('[class*="rounded-full"]');
      for (let i = 0; i < circles.length; i++) {
        if (circles[i] === activeCircle || circles[i].classList.toString().includes('bg-red')) {
          filledStepNum = i + 1;
          break;
        }
      }
    }

    // Find heading text
    const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4')).map(h => h.textContent.trim());
    const headingFound = headings.find(h => h.includes(heading));

    return {
      filledStepNum,
      headingFound: !!headingFound,
      allHeadings: headings,
      title: document.title
    };
  }, expectedStepNum, expectedHeading);

  console.log(`  Verification:`);
  console.log(`    - Stepper filled circle: Step ${info.filledStepNum} (expected ${expectedStepNum})`);
  console.log(`    - Heading "${expectedHeading}" found: ${info.headingFound}`);
  console.log(`    - Page title: ${info.title}`);

  return info;
}

async function takeScreenshot(page, stepNum, stepName, theme) {
  const filename = `step${stepNum}-${stepName}-${theme}.png`;
  const filepath = path.join(OUTPUT_DIR, filename);

  await page.screenshot({
    path: filepath,
    fullPage: true
  });

  console.log(`  ✓ Saved: ${filename}`);
  return filepath;
}

async function setTheme(page, theme) {
  await page.evaluate((t) => {
    localStorage.setItem('iot-twin-theme', t);
    // Also dispatch an event in case the app listens for theme changes
    window.dispatchEvent(new Event('storage'));
  }, theme);

  // Reload page to apply theme
  await page.reload({ waitUntil: 'networkidle2', timeout: 60000 });
  await sleep(500); // Additional wait for theme to apply
}

async function main() {
  let browser;

  try {
    browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();

    // Set viewport for full-page capture
    await page.setViewport({ width: 1280, height: 720 });

    console.log('Navigating to setup-validate...\n');

    // Process each step
    for (let i = 0; i < STEPS.length; i++) {
      const step = STEPS[i];
      console.log(`--- Step ${step.stepNum}: ${step.name.toUpperCase()} (step=${step.num}) ---`);

      // Navigate directly to the step
      const url = `http://localhost:3001/setup-validate?step=${step.num}`;
      console.log(`  Navigating to: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
      await sleep(1000);

      // Verify stepper and heading
      await verifyStepperAndHeading(page, step.stepNum, step.heading);

      // Special handling for step 4 (Validation - step=3)
      if (step.num === 3) {
        console.log(`  Step 4 (Validation): Clicking "Run validation" button...`);

        // Click the "Run validation" button
        const clicked = await page.evaluate(() => {
          const buttons = Array.from(document.querySelectorAll('button'));
          const btn = buttons.find(b =>
            b.textContent.includes('Run validation') ||
            b.textContent.includes('run') && b.textContent.toLowerCase().includes('validat')
          );
          if (btn) {
            btn.click();
            return true;
          }
          return false;
        });

        if (clicked) {
          console.log(`  Waiting 6 seconds for validation to show "Waiting" state...`);
          await sleep(6000);
        } else {
          console.log(`  Note: Could not find "Run validation" button, proceeding with screenshot`);
        }
      }

      // Dark theme screenshot
      console.log(`  Capturing dark theme...`);
      await takeScreenshot(page, step.stepNum, step.name, 'dark');

      // Switch to light theme
      console.log(`  Switching to light theme...`);
      await setTheme(page, 'light');
      await sleep(500); // Wait for theme transition

      // Light theme screenshot
      console.log(`  Capturing light theme...`);
      await takeScreenshot(page, step.stepNum, step.name, 'light');

      // Switch back to dark theme for next iteration
      if (i < STEPS.length - 1) {
        console.log(`  Switching back to dark theme...`);
        await setTheme(page, 'dark');
      }

      console.log('');
    }

    console.log('✅ All screenshots captured successfully!');
    console.log(`\nOutput directory: ${OUTPUT_DIR}`);

    // List all generated files
    const files = fs.readdirSync(OUTPUT_DIR)
      .filter(f => f.startsWith('step') && f.endsWith('.png'))
      .sort();

    console.log('\nGenerated files:');
    files.forEach(f => {
      const filepath = path.join(OUTPUT_DIR, f);
      const size = (fs.statSync(filepath).size / 1024).toFixed(2);
      console.log(`  - ${f} (${size} KB)`);
    });

    // Verify file count
    console.log(`\nTotal files: ${files.length} (expected 12)`);

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
