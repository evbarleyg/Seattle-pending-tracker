#!/usr/bin/env node
"use strict";

// Inject a fixed bottom-right pill into a built index.html so users can flip
// between the old and new style. Used by the deploy workflow on each push.
//
// Usage:
//   node scripts/inject_view_toggle.js <path-to-index.html> <target>
//     target: "new"  -> old-style page; pill links to ./new-style/
//             "old"  -> new-style page; pill links to ../

const fs = require("fs");
const path = require("path");

const SHARED_BASE = `position:fixed;bottom:18px;right:18px;z-index:9999;display:inline-flex;align-items:center;gap:6px;padding:10px 16px;border-radius:999px;text-decoration:none;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:13px;font-weight:600;box-shadow:0 4px 12px rgba(0,0,0,0.18);`;

const VARIANTS = {
  new: {
    href: "./new-style/",
    label: "Try the new view &rarr;",
    style: `${SHARED_BASE}background:#111827;color:#fff;border:1px solid rgba(255,255,255,0.1);`,
  },
  old: {
    href: "../",
    label: "&larr; Back to original view",
    style: `${SHARED_BASE}background:#fff;color:#111827;border:1px solid rgba(0,0,0,0.15);`,
  },
};

function main() {
  const target = process.argv[2];
  const variantKey = process.argv[3];
  if (!target || !variantKey || !VARIANTS[variantKey]) {
    console.error("Usage: inject_view_toggle.js <path-to-index.html> <new|old>");
    process.exit(1);
  }
  if (!fs.existsSync(target)) {
    console.error(`File not found: ${target}`);
    process.exit(1);
  }
  const v = VARIANTS[variantKey];
  const html = fs.readFileSync(target, "utf8");
  if (html.includes('data-view-toggle="injected"')) {
    console.log(`Toggle already present in ${path.basename(target)} — skipping.`);
    return;
  }
  const pill = `\n<a data-view-toggle="injected" href="${v.href}" style="${v.style}">${v.label}</a>\n`;
  const updated = html.includes("</body>")
    ? html.replace("</body>", `${pill}</body>`)
    : html + pill;
  fs.writeFileSync(target, updated);
  console.log(`Injected '${variantKey}' view-toggle into ${path.basename(target)}`);
}

main();
