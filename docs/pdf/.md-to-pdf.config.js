// Config for md-to-pdf to render Mermaid diagrams cleanly across pages.
//
// Three things this file does:
//   1. Swap ```mermaid fenced code blocks into <div class="mermaid">…</div>
//      so Mermaid.js can find and render them.
//   2. Inject Mermaid.js + an init script, and signal completion via a
//      window flag so md-to-pdf knows when it's safe to print.
//   3. Apply CSS that keeps each diagram intact across page breaks and
//      caps its height so a diagram never overflows a single page.

module.exports = {
  marked_extensions: [
    {
      renderer: {
        code(code, lang) {
          if (lang === "mermaid") {
            // Wrap each diagram in a figure so the page-break rule
            // applies to the whole block (caption + svg).
            return `<figure class="mermaid-figure"><div class="mermaid">${code}</div></figure>`;
          }
          return false;
        },
      },
    },
  ],
  script: [
    {
      url: "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js",
    },
    {
      content: `
        mermaid.initialize({
          startOnLoad: false,
          theme: 'default',
          flowchart: { useMaxWidth: true, htmlLabels: true },
          themeVariables: { fontSize: '14px' }
        });
        (async () => {
          await mermaid.run({ querySelector: '.mermaid' });
          // Resize any oversized SVGs so they fit on a portrait A4 page
          // (max ~ 240mm tall after margins).
          document.querySelectorAll('.mermaid svg').forEach(svg => {
            svg.style.maxWidth = '100%';
            svg.style.maxHeight = '220mm';
            svg.removeAttribute('height');
          });
          window.MERMAID_DONE = true;
        })();
      `,
    },
  ],
  css: `
    /* Keep diagrams from being split across pages. */
    figure.mermaid-figure {
      page-break-inside: avoid;
      break-inside: avoid;
      margin: 1.2em 0;
      text-align: center;
    }
    figure.mermaid-figure .mermaid {
      page-break-inside: avoid;
      break-inside: avoid;
      max-width: 100%;
    }
    figure.mermaid-figure svg {
      max-width: 100%;
      height: auto !important;
    }
    /* Keep section headings glued to the next paragraph (no orphaned
       headings at the bottom of a page). */
    h1, h2, h3 {
      page-break-after: avoid;
      break-after: avoid;
    }
    /* Keep tables together when possible. */
    table {
      page-break-inside: avoid;
      break-inside: avoid;
    }
    /* Slightly tighter body spacing so the doc fits in fewer pages. */
    body {
      font-family: -apple-system, system-ui, sans-serif;
      line-height: 1.5;
    }
  `,
  pdf_options: {
    format: "A4",
    margin: "18mm 15mm",
    printBackground: true,
  },
  launch_options: { args: ["--no-sandbox"] },
  wait_for: () => window.MERMAID_DONE === true,
};
