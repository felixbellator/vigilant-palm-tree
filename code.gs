/***** CONFIG *****/
const NETSKOPE_URL = 'https://<tenant>.goskope.com/api/v2/<private-apps-endpoint>';
// If the API uses Authorization, set TOKEN_HEADER='Authorization' and TOKEN='Bearer <token>'
const TOKEN_HEADER = 'Netskope-Api-Token';
const TOKEN       = '<YOUR_TOKEN>';

const SHEET1_NAME = 'sheet 1'; // original list in Column B (B:B)
const SHEET2_NAME = 'sheet 2'; // to be filled: Col A = App Name, Col B = Destination Hostname(s)

/**
 * Main entry: fetch Netskope NPA apps and write App Name + Dest Hostnames into "sheet 2".
 */
function updateSheet2FromNetskope() {
  const data = fetchNetskopeJson_(NETSKOPE_URL, TOKEN_HEADER, TOKEN);
  const rows = extractAppAndHosts_(data); // [["Application Name","Destination Hostnames"], ...]
  writeToSheet2_(rows);
  SpreadsheetApp.getActive().toast(`Updated ${SHEET2_NAME} with ${rows.length - 1} rows.`, 'Netskope Sync', 6);
}

/*** HTTP fetch ***/
function fetchNetskopeJson_(url, headerKey, headerValue) {
  const resp = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: { [headerKey]: headerValue },
    muteHttpExceptions: true
  });
  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error(`Netskope API error ${code}: ${resp.getContentText()}`);
  }
  const text = resp.getContentText();
  return JSON.parse(text);
}

/**
 * Heuristic extractor that looks for app "name" (or similar) and associated destination hostnames.
 * It is forgiving across payload shapes:
 * - app name keys: name, app_name, application, application_name
 * - host keys (string or arrays/objects): destination, destination_fqdn, fqdn, hostname, host, domain, domains,
 *   destinations[].{fqdn|hostname|host|domain}, resources[].{fqdn|hostname|host|domain}
 */
function extractAppAndHosts_(data) {
  const nameKeys = new Set(['name','app_name','application','application_name']);
  const hostKeyCandidates = new Set([
    'destination','destination_fqdn','fqdn','hostname','host','domain','domains','destinations','resources'
  ]);

  const pairs = []; // {name: string, hosts: Set<string>}

  function toHostList(value) {
    const out = new Set();

    function harvest(v) {
      if (v == null) return;
      if (typeof v === 'string') {
        const s = v.trim();
        if (s) out.add(s);
        return;
      }
      if (Array.isArray(v)) {
        v.forEach(harvest);
        return;
      }
      if (typeof v === 'object') {
        // Try common leaf keys
        for (const k of ['fqdn','hostname','host','domain','destination','destination_fqdn']) {
          if (k in v) harvest(v[k]);
        }
        // Also scan nested objects just in case
        for (const [_, vv] of Object.entries(v)) {
          if (vv && (typeof vv === 'object' || Array.isArray(vv))) harvest(vv);
        }
      }
    }
    harvest(value);
    return out;
  }

  function harvestFromAppObject(obj) {
    // Find the name
    let appName = null;
    for (const [k, v] of Object.entries(obj)) {
      if (typeof v === 'string' && nameKeys.has(k.toLowerCase())) {
        appName = v.trim();
        break;
      }
    }
    if (!appName) return;

    // Collect hostnames from any plausible fields in this object
    const hosts = new Set();
    for (const [k, v] of Object.entries(obj)) {
      if (hostKeyCandidates.has(k.toLowerCase())) {
        for (const h of toHostList(v)) hosts.add(h);
      }
    }

    // If hosts still empty, look slightly deeper (common sub-objects: "destinations", "resources")
    for (const childKey of ['destinations','resources']) {
      if (obj[childKey]) {
        for (const h of toHostList(obj[childKey])) hosts.add(h);
      }
    }

    if (appName) {
      pairs.push({ name: appName, hosts });
    }
  }

  function walk(o) {
    if (o == null) return;
    if (Array.isArray(o)) {
      o.forEach(walk);
      return;
    }
    if (typeof o === 'object') {
      // If this object looks like an app container (has a name-ish key), harvest it as a unit
      const hasNameish = Object.keys(o).some(k => nameKeys.has(k.toLowerCase()));
      if (hasNameish) {
        harvestFromAppObject(o);
      }
      // Also continue walking (in case apps are nested)
      for (const v of Object.values(o)) walk(v);
    }
  }

  walk(data);

  // Build rows for the sheet
  const header = ['Application Name', 'Destination Hostnames'];
  // de-dup by app name; merge hosts if we saw the app multiple times
  const map = new Map();
  for (const p of pairs) {
    const key = p.name.trim();
    if (!key) continue;
    const existing = map.get(key) || new Set();
    p.hosts.forEach(h => existing.add(h));
    map.set(key, existing);
  }

  const rows = [header];
  const sortedNames = Array.from(map.keys()).sort((a,b) => a.toLowerCase().localeCompare(b.toLowerCase()));
  for (const appName of sortedNames) {
    const hosts = Array.from(map.get(appName) || []);
    // Sort hosts for stable output; join with comma+space
    hosts.sort((a,b) => a.toLowerCase().localeCompare(b.toLowerCase()));
    rows.push([appName, hosts.join(', ')]);
  }
  return rows;
}

/*** Write to "sheet 2" ***/
function writeToSheet2_(rows) {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(SHEET2_NAME) || ss.insertSheet(SHEET2_NAME);

  // Clear existing
  sh.clear({ contentsOnly: true });
  if (rows.length === 0) return;

  // Resize and set values
  sh.getRange(1, 1, rows.length, rows[0].length).setValues(rows);

  // Nice header style
  const headerRange = sh.getRange(1, 1, 1, rows[0].length);
  headerRange.setFontWeight('bold');
  sh.autoResizeColumns(1, rows[0].length);
}
