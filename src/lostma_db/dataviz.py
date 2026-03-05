import re
import json
from html import escape
from IPython.display import HTML
import uuid


def dataviz_overview(
        styler,
        title="Overview",
        sections=None,
        default_open=None,
):
    """
    Build a fancy dataviz of the data overview
    """
    if sections is None:
        raise ValueError("sections must be provided")
    if default_open is None:
        default_open = set()
    else:
        default_open = set(default_open)

    html = styler.to_html()

    m = re.search(r"(<tbody>)(.*?)(</tbody>)", html, flags=re.DOTALL)
    if not m:
        raise ValueError("Could not find <tbody> in Styler HTML")

    tbody_open, tbody_inner, tbody_close = m.group(1), m.group(2), m.group(3)

    rows = re.findall(r"<tr.*?>.*?</tr>", tbody_inner, flags=re.DOTALL)

    row_label_re = re.compile(r'<th[^>]*class="row_heading[^"]*"[^>]*>(.*?)</th>', re.DOTALL)

    def get_label(tr_html: str) -> str:
        mm = row_label_re.search(tr_html)
        if not mm:
            return ""
        label_html = mm.group(1)
        label_text = re.sub(r"<.*?>", "", label_html).strip()
        return label_text

    def section_id_for(label: str):
        for s in sections:
            if s["match"](label):
                return s["id"]
        return None

    new_rows = []
    inserted = set()

    for tr in rows:
        label = get_label(tr)
        sid = section_id_for(label)

        if sid == "summary":
            tr = re.sub(r"<tr\b", '<tr data-summary="true"', tr, count=1)
            new_rows.append(tr)
            continue

        if sid is not None:
            if sid not in inserted:
                inserted.add(sid)
                tr = re.sub(
                    r"<tr\b",
                    f'<tr class="tg-section" data-tg-section="{escape(sid)}"',
                    tr,
                    count=1,
                )
                new_rows.append(tr)
                continue
            tr = re.sub(r"<tr\b", f'<tr data-section="{escape(sid)}"', tr, count=1)

        new_rows.append(tr)

    new_tbody = tbody_open + "\n".join(new_rows) + tbody_close
    html = html[:m.start()] + new_tbody + html[m.end():]
    default_open_js = json.dumps(sorted(default_open))
    uid = f"tg-{uuid.uuid4().hex}"

    script = f"""
    <script>
    (function() {{
      const DEFAULT_OPEN = {default_open_js};
      const wrap = document.getElementById("{uid}");
      if (!wrap) return;

      function setHeaderAndSummarySticky() {{
        const thead = wrap.querySelector("thead");
        const headerH = thead ? Math.ceil(thead.getBoundingClientRect().height) : 0;

        const summaryRows = wrap.querySelectorAll('tr[data-summary="true"]');
        if (!summaryRows.length) return;

        let offset = headerH;
        summaryRows.forEach((tr, i) => {{
          const cells = tr.querySelectorAll("th, td");
          const rowH = Math.ceil(tr.getBoundingClientRect().height) || 0;
          cells.forEach(cell => {{
            cell.style.top = offset + "px";
            cell.style.zIndex = 950 - i;
            cell.style.background = "#fafafa";
          }});
          offset += rowH;
        }});
      }}

      function setOpen(sectionId, open) {{
        const rows = wrap.querySelectorAll('tr[data-section="' + sectionId + '"]');
        rows.forEach(r => r.style.display = open ? "" : "none");
        const header = wrap.querySelector('tr.tg-section[data-tg-section="' + sectionId + '"]');
        if (header) header.dataset.open = open ? "true" : "false";
      }}

      function toggle(sectionId) {{
        const rows = wrap.querySelectorAll('tr[data-section="' + sectionId + '"]');
        if (!rows.length) return;
        const header = wrap.querySelector('tr.tg-section[data-tg-section="' + sectionId + '"]');
        const isOpen = header && header.dataset.open === "true";
        setOpen(sectionId, !isOpen);
      }}

      function applyInitialState() {{
        wrap.querySelectorAll('tr.tg-section[data-tg-section]').forEach(tr => {{
          const sid = tr.dataset.tgSection;
          setOpen(sid, DEFAULT_OPEN.includes(sid));
        }});
      }}

      wrap.querySelectorAll('tr.tg-section[data-tg-section]').forEach(tr => {{
        const sid = tr.dataset.tgSection;
        tr.addEventListener("click", () => {{
          toggle(sid);
          requestAnimationFrame(setHeaderAndSummarySticky);
        }});
      }});

      const openAll = document.getElementById("{uid}-open-all");
      const closeAll = document.getElementById("{uid}-close-all");

      if (openAll) openAll.addEventListener("click", () => {{
        wrap.querySelectorAll('tr.tg-section[data-tg-section]').forEach(tr => setOpen(tr.dataset.tgSection, true));
        requestAnimationFrame(setHeaderAndSummarySticky);
      }});

      if (closeAll) closeAll.addEventListener("click", () => {{
        wrap.querySelectorAll('tr.tg-section[data-tg-section]').forEach(tr => setOpen(tr.dataset.tgSection, false));
        requestAnimationFrame(setHeaderAndSummarySticky);
      }});

      requestAnimationFrame(() => {{
        applyInitialState();
        setHeaderAndSummarySticky();
      }});

      window.addEventListener("resize", setHeaderAndSummarySticky);
    }})();
    </script>
    """

    style = """
    <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 16px; }
    th.row_heading { text-align: right; padding-right: 10px; white-space: nowrap; }

    .tg-wrap tbody th,
    .tg-wrap tbody td{
      font-weight: 400;
    }

    tr[data-summary="true"] > th.row_heading{
      font-weight: 700;
    }

    tr.tg-section > th.row_heading{
      font-weight: 700;
      letter-spacing: 0.02em;
    }

    tr[data-section] > th.row_heading{
      font-weight: 400;
    }

    tr.tg-section > th.row_heading::before{
      content: "▸";
      display: inline-block;
      width: 1.2em;
      text-align: center;
      margin-right: 6px;
    }

    tr.tg-section[data-open="true"] > th.row_heading::before{
      content: "▾";
    }

    .tg-wrap{
      max-height: 70vh;
      overflow: auto;
      border: 1px solid #eee;
      border-radius: 10px;
    }

    .tg-wrap table{
      border-collapse: separate !important;
      border-spacing: 0;
    }
    
    .tg-wrap th,
    .tg-wrap td{
      padding: 6px 10px;
    }

    .tg-wrap thead th{
      position: sticky;
      top: 0;
      z-index: 1000;
      background: white;
      box-shadow: 0 1px 0 rgba(0,0,0,0.08);
      white-space: normal;
      line-height: 1.2;
    }
        
    .tg-wrap tbody td{
      min-width: 90px;
    }
    
    .tg-wrap th.row_heading{
      min-width: 220px;
    }

    tr[data-summary="true"] > th,
    tr[data-summary="true"] > td{
      position: sticky;
      z-index: 950;
      background: #fafafa;
    }
  </style>
    """

    final = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{escape(title)}</title>
  {style}
</head>
<body>
  <h1>{escape(title)}</h1>

  <div style="margin: 8px 0 14px; display:flex; gap:8px; flex-wrap: wrap;">
    <button id="{uid}-open-all">Open all</button>
    <button id="{uid}-close-all">Close all</button>
  </div>
<div id="{uid}" class="tg-wrap">
  {html}
 </div>
  {script}

</body>
</html>
"""
    return HTML(final)