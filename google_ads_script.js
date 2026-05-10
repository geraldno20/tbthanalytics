/**
 * Google Ads Script — Export campaign performance to Google Sheets
 *
 * Setup:
 * 1. Go to Google Ads → Tools → Scripts
 * 2. Create new script, paste this code
 * 3. Run once to test, then schedule daily
 *
 * The script exports per-video ad performance (YouTube video campaigns).
 * Your dashboard fetches from this sheet via the fetch_ads_from_sheet.py script.
 */

var SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1PoUfMdia4D78XlpmOfQUogsGAfLVslvxokcKXD5Ylts/edit?usp=sharing';
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

  // Date range: last 90 days
  var today = new Date();
  var start = new Date(today.getTime() - 90 * 24 * 60 * 60 * 1000);
  var startStr = Utilities.formatDate(start, AdsApp.currentAccount().getTimeZone(), 'yyyyMMdd');
  var endStr = Utilities.formatDate(today, AdsApp.currentAccount().getTimeZone(), 'yyyyMMdd');

  // Use AWQL report (well-supported in Google Ads Scripts)
  var report = AdsApp.report(
    'SELECT CampaignName, VideoId, VideoTitle, Cost, Impressions, VideoViews, Clicks, Date ' +
    'FROM VIDEO_PERFORMANCE_REPORT ' +
    'WHERE Date >= ' + startStr + ' AND Date <= ' + endStr
  );

  var rows = report.rows();
  var videoMap = {};

  while (rows.hasNext()) {
    var row = rows.next();
    var cost = parseFloat(row['Cost'].replace(/,/g, '')) || 0;
    var impressions = parseInt(row['Impressions'].replace(/,/g, '')) || 0;
    var views = parseInt(row['VideoViews'].replace(/,/g, '')) || 0;
    var clicks = parseInt(row['Clicks'].replace(/,/g, '')) || 0;

    sheet.appendRow([
      row['CampaignName'],
      row['VideoId'],
      row['VideoTitle'],
      cost,
      impressions,
      views,
      clicks,
      row['Date']
    ]);

    // Aggregate by video
    var vid = row['VideoId'];
    if (!videoMap[vid]) {
      videoMap[vid] = { id: vid, title: row['VideoTitle'], cost: 0, impressions: 0, views: 0, clicks: 0 };
    }
    videoMap[vid].cost += cost;
    videoMap[vid].impressions += impressions;
    videoMap[vid].views += views;
    videoMap[vid].clicks += clicks;
  }

  // Summary sheet
  var summarySheet = spreadsheet.getSheetByName('AdsSummary');
  if (!summarySheet) {
    summarySheet = spreadsheet.insertSheet('AdsSummary');
  }
  summarySheet.clear();
  summarySheet.appendRow(['Video ID', 'Video Title', 'Total Cost', 'Total Impressions', 'Total Views', 'Total Clicks']);

  var videos = Object.values(videoMap);
  videos.sort(function(a, b) { return b.cost - a.cost; });

  for (var i = 0; i < videos.length; i++) {
    var v = videos[i];
    summarySheet.appendRow([v.id, v.title, v.cost.toFixed(2), v.impressions, v.views, v.clicks]);
  }

  Logger.log('Exported ' + videos.length + ' videos to sheet.');
}
