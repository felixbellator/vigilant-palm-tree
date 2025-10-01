/***** CONFIG *****/
const NETSKOPE_URL      = 'https://<tenant>.goskope.com/api/v2/<private-apps-endpoint>';
const TOKEN             = 'YOUR_API_TOKEN_OR_BEARER_TOKEN';
const TOKEN_HEADER      = 'Netskope-Api-Token'; // or 'Authorization' if you pass `Bearer <token>`
const XLSX_FILE_ID      = 'YOUR_XLSX_FILE_ID';  // Drive file ID of the source XLSX
const SHEET_NAME        = 'Applications';       // set to null to use the first sheet
const COLUMN_NAME       = 'Application Name';   // header text OR numeric index as string ('0' for first col)
const OUTPUT_FOLDER_NAME = 'Netskope_NPA_Compare_Outputs'; // script will create/use a folder with this name

/***** MAIN *****/
function main() {
  const ts = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd_HHmmss");
  const outFolder = getOrCreateFolderByName_(OUTPUT_FOLDER_NAME);

  // 1) Fetch Netskope JSON and save to Drive
  const netskopeData = fetchNetskopeJson_(NETSKOPE_URL, TOKEN, TOKEN_HEADER);
  const jsonBlob = Utilities.newBlob(
    JSON.stringify(netskopeData, null, 2), 'application/json', `netskope_private_apps_${ts}.json`
  );
  const rawJsonFile = outFolder.createFile(jsonBlob);
  Logger.log(`Saved Netskope JSON: ${rawJsonFile.getUrl()}`);

  // 2) Parse application names from Netskope JSON
  const cloudNames = extractAppNamesFromJson_(netskopeData);
  Logger.log(`Parsed ${cloudNames.length} app names from Netskope payload`);

  // 3) Read XLSX from Drive (convert to Google Sheet if needed) & extract application names column
  const sheetId = convertXlsxToGoogleSheetIfNeeded_(XLSX_FILE_ID);
  const fileNames = readAppNamesFromSheet_(sheetId, SHEET_NAME, COLUMN_NAME);
  Logger.log(`Read ${fileNames.length} app names from spreadsheet`);

  // Normalize sets
  const fileNormSet = new Set(fileNames.map(normalize_));
  const cloudNormSet = new Set(cloudNames.map(normalize_));

  // 4i) Apps present in file but not in Netskope
  const missing = fileNames
    .filter(n => !cloudNormSet.has(normalize_(n)))
    .sort((a, b) => normalize_(a).localeCompare(normalize_(b)));

  // 4ii) Side-by-side compare (sorted), plus presence matrix
  const fileSorted = [...new Set(fileNames)].sort((a, b) => normalize_(a).localeCompare(normalize_(b)));
  const cloudSorted = [...new Set(cloudNames)].sort((a, b) => normalize_(a).localeCompare(normalize_(b)));

  // Write outputs
  writeTextFile_(outFolder, `apps_in_file_not_in_netskope_${ts}.txt`, missing.join('\n'));

  const sideBySideCsv = buildSideBySideCsv_(fileSorted, cloudSorted, 'From_File', 'From_Netskope');
  writeTextFile_(outFolder, `comparison_side_by_side_${ts}.csv`, sideBySideCsv, 'text/csv');

  const presenceCsv = buildPresenceMatrixCsv_(fileNormSet, cloudNormSet);
  writeTextFile_(outFolder, `presence_matrix_${ts}.csv`, presenceCsv, 'text/csv');

  Logger.log('Done.');
}

/***** Netskope fetch *****/
function fetchNetskopeJson_(url, token, tokenHeader) {
  const headers = {};
  headers[tokenHeader] = token;

  const resp = UrlFetchApp.fetch(url, {
    method: 'get',
    headers,
    muteHttpExceptions: true
  });
  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error(`Netskope API error ${code}: ${resp.getContentText()}`);
  }
  const contentType = resp.getHeaders()['Content-Type'] || '';
  const text = resp.getContentText();

  if (contentType.includes('application/json') || text.trim().startsWith('{') || text.trim().startsWith('[')) {
    return JSON.parse(text);
  }
  throw new Error('Unexpected response (not JSON).');
}

/***** JSON app name extraction (recursive & forgiving) *****/
function extractAppNamesFromJson_(data) {
  const candidateKeys = new Set(['name','app_name','application','application_name']);
  const out = [];

  (function walk(obj) {
    if (obj === null || obj === undefined) return;
    if (Array.isArray(obj)) {
      obj.forEach(walk);
    } else if (typeof obj === 'object') {
      for (const [k, v] of Object.entries(obj)) {
        if (typeof v === 'string' && candidateKeys.has(String(k).toLowerCase())) {
          const s = v.trim();
          if (s) out.push(s);
        } else {
          walk(v);
        }
      }
    }
  })(data);

  // De-dup & return
  return Array.from(new Set(out));
}

/***** XLSX → Google Sheet (if needed) *****/
function convertXlsxToGoogleSheetIfNeeded_(fileId) {
  const file = DriveApp.getFileById(fileId);
  const mime = file.getMimeType();
  if (mime === MimeType.GOOGLE_SHEETS) {
    return fileId; // already a sheet
  }
  // Convert via Advanced Drive API
  const copied = Drive.Files.copy(
    { title: `${file.getName()} (Converted)` , mimeType: MimeType.GOOGLE_SHEETS },
    fileId
  );
  Logger.log(`Converted XLSX to Google Sheet: https://docs.google.com/spreadsheets/d/${copied.id}/edit`);
  return copied.id;
}

/***** Read app names column from Google Sheet *****/
function readAppNamesFromSheet_(spreadsheetId, sheetName, columnNameOrIndex) {
  const ss = SpreadsheetApp.openById(spreadsheetId);
  const sh = sheetName ? ss.getSheetByName(sheetName) : ss.getSheets()[0];
  if (!sh) throw new Error(`Sheet '${sheetName}' not found`);

  const range = sh.getDataRange();
  const values = range.getValues(); // 2D array

  if (!values.length) return [];

  // Determine target column index
  let targetCol = 0;
  if (columnNameOrIndex == null) {
    targetCol = 0; // default first column
  } else {
    const maybeIdx = parseInt(columnNameOrIndex, 10);
    if (!isNaN(maybeIdx)) {
      targetCol = maybeIdx; // zero-based
    } else {
      // header lookup (first row)
      const headers = values[0].map(h => String(h).trim());
      const idx = headers.findIndex(h => h.toLowerCase() === String(columnNameOrIndex).trim().toLowerCase());
      if (idx === -1) {
        throw new Error(`Column '${columnNameOrIndex}' not found. Headers: ${JSON.stringify(headers)}`);
      }
      targetCol = idx;
      // drop header row for data extraction
      values.shift();
    }
  }

  const names = [];
  for (const row of values) {
    const cell = (row[targetCol] != null) ? String(row[targetCol]).trim() : '';
    if (cell) names.push(cell);
  }
  return names;
}

/***** Output builders *****/
function buildSideBySideCsv_(leftArr, rightArr, leftHeader, rightHeader) {
  const rows = [[leftHeader, rightHeader]];
  const maxLen = Math.max(leftArr.length, rightArr.length);
  for (let i = 0; i < maxLen; i++) {
    rows.push([leftArr[i] || '', rightArr[i] || '']);
  }
  return toCsv_(rows);
}

function buildPresenceMatrixCsv_(fileNormSet, cloudNormSet) {
  const union = new Set([...fileNormSet, ...cloudNormSet]);
  const apps = Array.from(union).sort();
  const rows = [['Application','In_File','In_Netskope']];
  for (const a of apps) {
    rows.push([a, fileNormSet.has(a) ? 'Yes' : 'No', cloudNormSet.has(a) ? 'Yes' : 'No']);
  }
  return toCsv_(rows);
}

/***** Utils *****/
function toCsv_(rows) {
  return rows.map(r =>
    r.map(field => {
      const s = String(field ?? '');
      // escape quotes, wrap if contains comma/quote/newline
      const needsWrap = /[",\n]/.test(s);
      const escaped = s.replace(/"/g, '""');
      return needsWrap ? `"${escaped}"` : escaped;
    }).join(',')
  ).join('\n');
}

function writeTextFile_(folder, name, content, mimeType) {
  const blob = Utilities.newBlob(content, mimeType || 'text/plain', name);
  const f = folder.createFile(blob);
  Logger.log(`Wrote: ${f.getUrl()}`);
  return f;
}

function normalize_(s) {
  return String(s).trim().replace(/\s+/g, ' ').toLowerCase();
}

function getOrCreateFolderByName_(name) {
  const it = DriveApp.getFoldersByName(name);
  if (it.hasNext()) return it.next();
  return DriveApp.createFolder(name);
}
