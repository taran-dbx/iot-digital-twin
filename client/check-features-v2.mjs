import puppeteer from 'puppeteer';

const BASE_URL = 'http://localhost:3001';

async function checkFeatures() {
  let browser;

  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1440, height: 1080 });

    // Check setup-validate page with detailed styling
    console.log('\n=== DETAILED INDUSTRY GRID CHECK ===');
    await page.goto(`${BASE_URL}/setup-validate`, { waitUntil: 'networkidle2', timeout: 10000 });
    await new Promise(r => setTimeout(r, 2000));

    const industryDetails = await page.evaluate(() => {
      const result = {
        industries: [],
        raw: ''
      };

      // Get the entire setup-validate content for inspection
      const mainContent = document.body.innerText;
      result.raw = mainContent.substring(0, 1000);

      // Look for all clickable industry elements
      const allElements = document.querySelectorAll('button, [role="button"], [onclick], .cursor-pointer, [class*="industry"], [class*="card"]');
      
      allElements.forEach(el => {
        const text = (el.textContent || '').trim();
        if (text.match(/Manufacturing|Energy|Pharma|Finance|Healthcare/i)) {
          const style = window.getComputedStyle(el);
          const parentStyle = el.parentElement ? window.getComputedStyle(el.parentElement) : null;
          
          result.industries.push({
            text: text.substring(0, 100),
            opacity: style.opacity,
            pointerEvents: style.pointerEvents,
            cursor: style.cursor,
            filter: style.filter,
            parentOpacity: parentStyle?.opacity,
            disabled: el.hasAttribute('disabled'),
            ariaDisabled: el.getAttribute('aria-disabled'),
            className: el.className
          });
        }
      });

      return result;
    });

    console.log('Found industries:');
    industryDetails.industries.forEach((ind, idx) => {
      console.log(`\n${idx + 1}. "${ind.text}"`);
      console.log(`   Opacity: ${ind.opacity}, Filter: ${ind.filter}`);
      console.log(`   Cursor: ${ind.cursor}, Disabled: ${ind.disabled}, aria-disabled: ${ind.ariaDisabled}`);
      console.log(`   Pointer Events: ${ind.pointerEvents}`);
    });

    // More granular check
    const manufacturingDisabled = industryDetails.industries.find(i => i.text.includes('Manufacturing'));
    const energyDisabled = industryDetails.industries.find(i => i.text.includes('Energy'));

    console.log('\n' + '='.repeat(80));
    console.log('INDUSTRY STATE SUMMARY');
    console.log('='.repeat(80));

    if (manufacturingDisabled) {
      const isDisabled = manufacturingDisabled.disabled || 
                        manufacturingDisabled.ariaDisabled === 'true' ||
                        manufacturingDisabled.pointerEvents === 'none' ||
                        parseFloat(manufacturingDisabled.opacity) < 0.7 ||
                        manufacturingDisabled.filter.includes('grayscale') ||
                        manufacturingDisabled.cursor === 'not-allowed';
      console.log(`Manufacturing disabled/greyed: ${isDisabled ? 'YES' : 'NO'}`);
      console.log(`  (opacity: ${manufacturingDisabled.opacity}, aria-disabled: ${manufacturingDisabled.ariaDisabled}, disabled attr: ${manufacturingDisabled.disabled})`);
    }

    if (energyDisabled) {
      const isDisabled = energyDisabled.disabled || 
                        energyDisabled.ariaDisabled === 'true' ||
                        energyDisabled.pointerEvents === 'none' ||
                        parseFloat(energyDisabled.opacity) < 0.7 ||
                        energyDisabled.filter.includes('grayscale') ||
                        energyDisabled.cursor === 'not-allowed';
      console.log(`Energy disabled/greyed: ${isDisabled ? 'YES' : 'NO'}`);
      console.log(`  (opacity: ${energyDisabled.opacity}, aria-disabled: ${energyDisabled.ariaDisabled}, disabled attr: ${energyDisabled.disabled})`);
    }

    await browser.close();

  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

checkFeatures();
