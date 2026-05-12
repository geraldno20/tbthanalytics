/**
 * Google Ads Script — Export campaign performance to Google Sheets
 *
 * Setup:
 * 1. Go to Google Ads → Tools → Scripts
 * 2. Create new script, paste this code
 * 3. Run once to test, then schedule daily
 */

var SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1PoUfMdia4D78XlpmOfQUogsGAfLVslvxokcKXD5Ylts/edit?usp=sharing';

function main() {
  var spreadsheet = SpreadsheetApp.openByUrl(SPREADSHEET_URL);

  var startStr = '20250301';
  var endStr = Utilities.formatDate(new Date(), AdsApp.currentAccount().getTimeZone(), 'yyyyMMdd');

  // --- Sheet 1: Campaign Performance (all campaign types) ---
  var campSheet = spreadsheet.getSheetByName('Campaigns');
  if (!campSheet) campSheet = spreadsheet.insertSheet('Campaigns');
  campSheet.clear();
  campSheet.appendRow(['Campaign', 'Campaign Type', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Date']);

  var campReport = AdsApp.report(
    'SELECT CampaignName, AdvertisingChannelType, Cost, Impressions, Clicks, Conversions, Date ' +
    'FROM CAMPAIGN_PERFORMANCE_REPORT ' +
    'WHERE Date >= ' + startStr + ' AND Date <= ' + endStr
  );

  var campRows = campReport.rows();
  while (campRows.hasNext()) {
    var row = campRows.next();
    campSheet.appendRow([
      row['CampaignName'],
      row['AdvertisingChannelType'],
      row['Cost'],
      row['Impressions'],
      row['Clicks'],
      row['Conversions'],
      row['Date']
    ]);
  }

  // --- Sheet 2: Video Performance (if any) ---
  var videoSheet = spreadsheet.getSheetByName('AdsData');
  if (!videoSheet) videoSheet = spreadsheet.insertSheet('AdsData');
  videoSheet.clear();
  videoSheet.appendRow(['Campaign', 'Video ID', 'Video Title', 'Cost', 'Impressions', 'Views', 'Clicks', 'Date']);

  try {
    var videoReport = AdsApp.report(
      'SELECT CampaignName, VideoId, VideoTitle, Cost, Impressions, VideoViews, Clicks, Date ' +
      'FROM VIDEO_PERFORMANCE_REPORT ' +
      'WHERE Date >= ' + startStr + ' AND Date <= ' + endStr
    );

    var videoRows = videoReport.rows();
    while (videoRows.hasNext()) {
      var row = videoRows.next();
      videoSheet.appendRow([
        row['CampaignName'],
        row['VideoId'],
        row['VideoTitle'],
        row['Cost'],
        row['Impressions'],
        row['VideoViews'],
        row['Clicks'],
        row['Date']
      ]);
    }
  } catch (e) {
    Logger.log('VIDEO_PERFORMANCE_REPORT not available: ' + e);
  }

  // --- Sheet 3: Ad Performance (shows all ad types including video assets) ---
  var adSheet = spreadsheet.getSheetByName('AdPerformance');
  if (!adSheet) adSheet = spreadsheet.insertSheet('AdPerformance');
  adSheet.clear();
  adSheet.appendRow(['Campaign', 'Ad Group', 'Ad Type', 'Headline', 'Cost', 'Impressions', 'Clicks', 'Conversions', 'Date']);

  var adReport = AdsApp.report(
    'SELECT CampaignName, AdGroupName, AdType, HeadlinePart1, Cost, Impressions, Clicks, Conversions, Date ' +
    'FROM AD_PERFORMANCE_REPORT ' +
    'WHERE Date >= ' + startStr + ' AND Date <= ' + endStr
  );

  var adRows = adReport.rows();
  while (adRows.hasNext()) {
    var row = adRows.next();
    adSheet.appendRow([
      row['CampaignName'],
      row['AdGroupName'],
      row['AdType'],
      row['HeadlinePart1'],
      row['Cost'],
      row['Impressions'],
      row['Clicks'],
      row['Conversions'],
      row['Date']
    ]);
  }

  // --- Summary sheet (aggregate campaigns) ---
  var summarySheet = spreadsheet.getSheetByName('AdsSummary');
  if (!summarySheet) summarySheet = spreadsheet.insertSheet('AdsSummary');
  summarySheet.clear();
  summarySheet.appendRow(['Campaign', 'Campaign Type', 'Total Cost', 'Total Impressions', 'Total Clicks', 'Total Conversions']);

  var summaryReport = AdsApp.report(
    'SELECT CampaignName, AdvertisingChannelType, Cost, Impressions, Clicks, Conversions ' +
    'FROM CAMPAIGN_PERFORMANCE_REPORT ' +
    'WHERE Date >= ' + startStr + ' AND Date <= ' + endStr
  );

  var campaignMap = {};
  var summaryRows = summaryReport.rows();
  while (summaryRows.hasNext()) {
    var row = summaryRows.next();
    var name = row['CampaignName'];
    if (!campaignMap[name]) {
      campaignMap[name] = { name: name, type: row['AdvertisingChannelType'], cost: 0, impressions: 0, clicks: 0, conversions: 0 };
    }
    campaignMap[name].cost += parseFloat(row['Cost'].replace(/,/g, '')) || 0;
    campaignMap[name].impressions += parseInt(row['Impressions'].replace(/,/g, '')) || 0;
    campaignMap[name].clicks += parseInt(row['Clicks'].replace(/,/g, '')) || 0;
    campaignMap[name].conversions += parseFloat(row['Conversions'].replace(/,/g, '')) || 0;
  }

  var campaigns = Object.values(campaignMap);
  campaigns.sort(function(a, b) { return b.cost - a.cost; });

  for (var i = 0; i < campaigns.length; i++) {
    var c = campaigns[i];
    summarySheet.appendRow([c.name, c.type, c.cost.toFixed(2), c.impressions, c.clicks, c.conversions.toFixed(1)]);
  }

  Logger.log('Exported ' + campaigns.length + ' campaigns to sheet.');
}
