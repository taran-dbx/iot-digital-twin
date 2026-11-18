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
  { num: 1, name: 'flavor' },
  { num: 2, name: 'source' },
  { num: 3, name: 'preview' },
  { num: 4, name: 'validation' },
  { num: 5, name: 'metrics' },
  { num: 6, name: 'notebook' }
];

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function takeScreenshot(page, stepNum, stepName, theme) {
  const filename = `step${stepNum}-${stepName}-${theme}.png`;
  const filepath = path.join(OUTPUT_DIR, filename);

  await page.screenshot({
    path: filepath,
    fullPage: true
  });

  console.log(`✓ Saved: ${filename}`);
  return filepath;
}

async function setTheme(page, theme) {
  await page.evaluate((t) => {
    localStorage.setItem('iot-twin-theme', t);
  }, theme);

  // Reload page to apply theme
  await page.reload({ waitUntil: 'domcontentloaded', timeout: 60000 });
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

    console.log('Navigating to setup-validate...');
    await page.goto('http://localhost:3001/setup-validate', { waitUntil: 'domcontentloaded', timeout: 60000 });

    // Process each step
    for (let i = 0; i < STEPS.length; i++) {
      const step = STEPS[i];
      console.log(`\n--- Step ${step.num}: ${step.name.toUpperCase()} ---`);

      // Dark theme screenshot
      console.log(`Capturing dark theme...`);
      await takeScreenshot(page, step.num, step.name, 'dark');

      // Switch to light theme
      console.log(`Switching to light theme...`);
      await setTheme(page, 'light');
      await sleep(500); // Wait for theme transition

      // Light theme screenshot
      console.log(`Capturing light theme...`);
      await takeScreenshot(page, step.num, step.name, 'light');

      // Switch back to dark theme for next step
      console.log(`Switching back to dark theme...`);
      await setTheme(page, 'dark');
      await sleep(500);

      // Move to next step (except on last step)
      if (i < STEPS.length - 1) {
        console.log(`Advancing to next step...`);

        // Special handling for step 4 (Validation)
        if (step.num === 4) {
          console.log(`  Step 4: Waiting 6 seconds for validation to complete...`);
          await sleep(6000);

          // Check for yellow callout with "I've enabled the sync" button
          const calloutExists = await page.evaluate(() => {
            const buttons = Array.from(document.querySelectorAll('button'));
            return buttons.some(b => b.textContent.includes("I've enabled"));
          }).catch(() => false);

          if (calloutExists) {
            console.log(`  Found "I've enabled the sync" button, clicking it...`);
            await page.evaluate(() => {
              const buttons = Array.from(document.querySelectorAll('button'));
              const btn = buttons.find(b => b.textContent.includes("I've enabled"));
              if (btn) btn.click();
            });
            await sleep(1000);
          }
        }

        // Click "Next" button
        await page.evaluate(() => {
          const buttons = Array.from(document.querySelectorAll('button'));
          const nextBtn = buttons.find(b => b.textContent.includes('Next'));
          if (nextBtn) nextBtn.click();
        });

        await sleep(500);
        await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 10000 }).catch(() => {
          // Navigation might not occur, that's ok
        });
        await sleep(500);
      }
    }

    console.log('\n✅ All screenshots captured successfully!');
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
