"use strict";

// Pure-helper coverage for src/views/afford.mjs. The rendered markup itself
// needs a DOM and is exercised by hand in the browser (see the view's own
// gate instructions); this file only covers affordAssumptionsSentence, the
// one pure function extracted for the "surface the assumptions" treatment.

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

async function importModule(relativePath) {
  return import(pathToFileURL(path.resolve(__dirname, "..", relativePath)).href);
}

// Mirrors affordability.config.sample.json's housing block.
const SAMPLE_HOUSING_CONFIG = {
  housing: {
    mortgageRateBeforeDiscount: 0.065,
    bofaRateDiscount: 0.0075,
    loanTermYears: 30,
    propertyTaxAnnualRate: 0.01,
    insuranceMonthly: 300,
    hoaMonthly: 0,
    maintenanceAnnualRate: 0.01,
  },
};

test("affordAssumptionsSentence spells out rate, tax, insurance, HOA and maintenance from config", async () => {
  const { affordAssumptionsSentence } = await importModule("src/views/afford.mjs");
  const sentence = affordAssumptionsSentence(SAMPLE_HOUSING_CONFIG);

  assert.equal(
    sentence,
    "This decision assumes a 5.8% mortgage rate (6.5% base rate minus a 0.8% lender discount), " +
      "1.0% property tax, $300 a month for insurance, $0 a month for HOA dues and " +
      "1.0% of price a year set aside for maintenance, on a 30-year loan."
  );
});

test("affordAssumptionsSentence reflects a nonzero HOA and a shorter loan term", async () => {
  const { affordAssumptionsSentence } = await importModule("src/views/afford.mjs");
  const sentence = affordAssumptionsSentence({
    housing: {
      mortgageRateBeforeDiscount: 0.07,
      bofaRateDiscount: 0,
      loanTermYears: 15,
      propertyTaxAnnualRate: 0.012,
      insuranceMonthly: 220,
      hoaMonthly: 450,
      maintenanceAnnualRate: 0.015,
    },
  });

  assert.match(sentence, /7\.0% mortgage rate \(7\.0% base rate minus a 0\.0% lender discount\)/);
  assert.match(sentence, /\$450 a month for HOA dues/);
  assert.match(sentence, /on a 15-year loan\.$/);
});

test("affordAssumptionsSentence falls back to a 30-year term when loanTermYears is missing", async () => {
  const { affordAssumptionsSentence } = await importModule("src/views/afford.mjs");
  const sentence = affordAssumptionsSentence({
    housing: {
      mortgageRateBeforeDiscount: 0.06,
      bofaRateDiscount: 0.005,
      propertyTaxAnnualRate: 0.01,
      insuranceMonthly: 250,
      hoaMonthly: 0,
      maintenanceAnnualRate: 0.01,
    },
  });

  assert.match(sentence, /on a 30-year loan\.$/);
});
