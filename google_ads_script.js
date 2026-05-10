/**
 * Google Ads Script — Export campaign performance to Google Sheets
 *
 * Setup:
 * 1. Go to Google Ads → Tools → Scripts
 * 2. Create new script, paste this code
 * 3. Replace SPREADSHEET_URL with your Google Sheet URL
 * 4. Run once to test, then schedule daily
 *
 * The script exports per-video ad performance (YouTube video campaigns).
 * Your dashboard fetches from this sheet via the fetch_ads.py script.
 */

var SPREADSHEET_URL = 'YOUR_GOOGLE_SHEET_URL_HERE';
var SHEET_NAME = 'AdsData';

function main() {
  var spreadsheet = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var sheet = spreadsheet.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(SHEET_NAME);
  }

  // Clear existing data
  sheet.clear();

  // Headers
  sheet.appendRow([
    'Campaign', 'Video ID', 'Video Title', 'Cost', 'Impressions', 'Views', 'Clicks', 'Date'
  ]);

  // Query: Video campaign performance, last 90 days
  var query = 'SELECT campaign.name, video.id, video.title, ' +
    'metrics.cost_micros, metrics.impressions, metrics.video_views, metrics.clicks, ' +
    'segments.date ' +
    'FROM video ' +
    'WHERE segments.date DURING LAST_90_DAYS ' +
    'ORDER BY segments.date DESC';

  var report = AdsApp.search(query);

  while (report.hasNext()) {
    var row = report.next();
    sheet.appendRow([
      row.campaign.name,
      row.video.id,
      row.video.title,
      row.metrics.costMicros / 1000000,  // Convert micros to dollars
      row.metrics.impressions,
      row.metrics.videoViews,
      row.metrics.clicks,
      row.segments.date
    ]);
  }

  // Also add a summary sheet
  var summarySheet = spreadsheet.getSheetByName('AdsSummary');
  if (!summarySheet) {
    summarySheet = spreadsheet.insertSheet('AdsSummary');
  }
  summarySheet.clear();
  summarySheet.appendRow(['Video ID', 'Video Title', 'Total Cost', 'Total Impressions', 'Total Views', 'Total Clicks']);

  // Aggregate by video
  var videoMap = {};
  var query2 = 'SELECT video.id, video.title, ' +
    'metrics.cost_micros, metrics.impressions, metrics.video_views, metrics.clicks ' +
    'FROM video ' +
    'WHERE segments.date DURING LAST_90_DAYS';

  var report2 = AdsApp.search(query2);
  while (report2.hasNext()) {
    var row = report2.next();
    var vid = row.video.id;
    if (!videoMap[vid]) {
      videoMap[vid] = { id: vid, title: row.video.title, cost: 0, impressions: 0, views: 0, clicks: 0 };
    }
    videoMap[vid].cost += row.metrics.costMicros / 1000000;
    videoMap[vid].impressions += row.metrics.impressions;
    videoMap[vid].views += row.metrics.videoViews;
    videoMap[vid].clicks += row.metrics.clicks;
  }

  var videos = Object.values(videoMap);
  videos.sort(function(a, b) { return b.cost - a.cost; });

  for (var i = 0; i < videos.length; i++) {
    var v = videos[i];
    summarySheet.appendRow([v.id, v.title, v.cost.toFixed(2), v.impressions, v.views, v.clicks]);
  }

  Logger.log('Exported ' + videos.length + ' videos to sheet.');
}
