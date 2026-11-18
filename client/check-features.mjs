import puppeteer from 'puppeteer';

const BASE_URL = 'http://localhost:3001';

async function checkFeatures() {
  let browser;
  const findings = {};

  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1440, height: 1080 });

    // Check home page for [dev] toggle
    console.log('\n=== CHECKING HOME PAGE ===');
    await page.goto(`${BASE_URL}/`, { waitUntil: 'networkidle2', timeout: 10000 });
    await new Promise(r => setTimeout(r, 2000));

    const homeChecks = await page.evaluate(() => {
      // Check for [dev] toggle
      const allElements = document.querySelectorAll('*');
      let devToggleFound = false;
      for (let el of allElements) {
        if (el.textContent && el.textContent.includes('[dev]')) {
          devToggleFound = true;
          break;
        }
      }

      // Check for "All pipelines" button
      let allPipelinesFound = false;
      const buttons = document.querySelectorAll('button');
      for (let btn of buttons) {
        if (btn.textContent.includes('All pipelines')) {
          allPipelinesFound = true;
          break;
        }
      }

      return {
        devToggleFound,
        allPipelinesFound
      };
    });

    findings.devToggleRemoved = !homeChecks.devToggleFound;
    findings.allPipelinesVisible = homeChecks.allPipelinesFound;

    console.log(`✓ [dev] toggle removed: ${findings.devToggleRemoved ? 'YES ✓' : 'NO ✗ (still visible)'}`);
    console.log(`✓ "All pipelines" button visible: ${findings.allPipelinesVisible ? 'YES' : 'NO (expected - non-admin)'}`);

    // Check setup-validate page for greyed industries
    console.log('\n=== CHECKING SETUP-VALIDATE PAGE ===');
    await page.goto(`${BASE_URL}/setup-validate`, { waitUntil: 'networkidle2', timeout: 10000 });
    await new Promise(r => setTimeout(r, 2000));

    const industryChecks = await page.evaluate(() => {
      const result = {
        manufacturingGreyed: null,
        manufacturingLabel: null,
        energyGreyed: null,
        energyLabel: null
      };

      // Look for industry cards with text content
      const allDivs = document.querySelectorAll('div, button');
      
      for (let el of allDivs) {
        const text = el.textContent || '';
        const style = window.getComputedStyle(el);
        const opacity = parseFloat(style.opacity);
        const pointerEvents = style.pointerEvents;
        
        // Check for Manufacturing
        if (text.includes('Manufacturing')) {
          result.manufacturingGreyed = opacity < 0.6 || pointerEvents === 'none';
          result.manufacturingLabel = text.includes('already configured') || text.includes('edit');
        }
        
        // Check for Energy
        if (text.includes('Energy')) {
          result.energyGreyed = opacity < 0.6 || pointerEvents === 'none';
          result.energyLabel = text.includes('already configured') || text.includes('edit');
        }
      }

      return result;
    });

    console.log(`✓ Manufacturing industry greyed: ${industryChecks.manufacturingGreyed ? 'YES ✓' : industryChecks.manufacturingGreyed === null ? 'NOT FOUND' : 'NO'}`);
    console.log(`✓ Manufacturing has "already configured · edit" label: ${industryChecks.manufacturingLabel ? 'YES ✓' : 'NO'}`);
    console.log(`✓ Energy industry greyed: ${industryChecks.energyGreyed ? 'YES ✓' : industryChecks.energyGreyed === null ? 'NOT FOUND' : 'NO'}`);
    console.log(`✓ Energy has "already configured · edit" label: ${industryChecks.energyLabel ? 'YES ✓' : 'NO'}`);

    await browser.close();

    // Summary
    console.log('\n' + '='.repeat(80));
    console.log('KEY FINDINGS SUMMARY');
    console.log('='.repeat(80));
    console.log(`1. [dev] toggle removed: ${findings.devToggleRemoved ? 'YES ✓' : 'NO ✗'}`);
    console.log(`2. "All pipelines" button: ${findings.allPipelinesVisible ? 'VISIBLE (admin user)' : 'NOT VISIBLE (non-admin, expected)'}`);
    console.log(`3. Manufacturing greyed + labeled: ${industryChecks.manufacturingGreyed && industryChecks.manufacturingLabel ? 'YES ✓' : 'NO ✗'}`);
    console.log(`4. Energy greyed + labeled: ${industryChecks.energyGreyed && industryChecks.energyLabel ? 'YES ✓' : 'NO ✗'}`);
    console.log('='.repeat(80));

  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

checkFeatures();
