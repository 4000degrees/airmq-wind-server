const fs = require("fs");
const { exec } = require("child_process");
const moment = require("moment");
const https = require("https");
const express = require("express");

/* 
Fetches GFS 1.00 Degree data from nomads.ncep.noaa.gov and converts it to JSON.
Keeps last 5 6-hour GFS datasets and returns the closest one of the available to the provided time.
*/

const SERVER_PORT = 3333;
const GRIB_DATA_DIR = "grib";
const JSON_DATA_DIR = "json";
const GRIB2JSON_BIN_PATH = "converter/bin/grib2json";
const GFS_FILTER_URL =
  "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_1p00.pl";
const KEEP_LAST_N_DATASETS = 5;
const LOG_FILE = "log.txt";
const ENABLE_LOGGING = true;

function log(text) {
  const entry = `[ ${new Date().toISOString()} ] ${text}`;
  console.log(entry);

  if (!ENABLE_LOGGING) return;
  fs.appendFile("log.txt", `${entry}\n`, (error) => {
    if (error) console.error(error);
  });
}

/**
 * Get array of incremental numbers.
 * Used to get hour increments back in time to manage datasets.
 */
function getIncrementalArray(amount = KEEP_LAST_N_DATASETS, increment = 6) {
  return Array(amount)
    .fill()
    .map((_, i) => i * increment);
}

function getJsonFilePath(timestamp) {
  return `${JSON_DATA_DIR}/${timestamp}.json`;
}

function getGribFilePath(timestamp) {
  return `${GRIB_DATA_DIR}/${timestamp}.f000`;
}

/**
 * Uses java grib2json converter to convert downloaded data.
 * Java needs to be installed on the machine and binary available at /bin/java.
 * @param {String} gribFilePath - Relative path to input GRIB2 file
 * @param {String} jsonFilePath - Relative path to output JSON file
 * @returns {Promise}
 */
function convertGribToJson(gribFilePath, jsonFilePath) {
  log(`Converting ${gribFilePath}.`);
  return new Promise((resolve, reject) => {
    exec(
      `${GRIB2JSON_BIN_PATH} --data --output ${jsonFilePath} --names --compact ${gribFilePath}`,
      { maxBuffer: 500 * 1024 },
      (error) => {
        if (error) {
          log(error);
          reject(error);
        } else {
          // Don't keep raw grib data
          exec(`rm ${GRIB_DATA_DIR}/*`);
          log(`Converted ${gribFilePath} to ${jsonFilePath}.`);
          resolve(jsonFilePath);
        }
      }
    );
  });
}

/**
 * Fetches GFS data for specified date and cycle hour.
 * @param {string} date - YYYYMMDD formatted date string
 * @param {('00'|'06'|'12'|'18')} hour
 * @returns {Promise}
 */
function getGribData(date, hour) {
  const timestamp = date + hour;
  const gribFilePath = getGribFilePath(timestamp);

  log(`Fetching grib data for ${timestamp}.`);
  const query = {
    file: `gfs.t${hour}z.pgrb2.1p00.f000`,
    lev_10_m_above_ground: "on",
    lev_surface: "on",
    var_TMP: "on",
    var_UGRD: "on",
    var_VGRD: "on",
    leftlon: 0,
    rightlon: 360,
    toplat: 90,
    bottomlat: -90,
    dir: `/gfs.${date}/${hour}/atmos`,
  };
  const queryString = new URLSearchParams(query).toString();

  const promise = new Promise((resolve, reject) => {
    https
      .get(`${GFS_FILTER_URL}?${queryString}`, (response) => {
        if (response.statusCode === 404) {
          log(`Data for ${timestamp} does ont exist on the server.`);
        } else if (response.statusCode === 200) {
          const data = [];

          response.on("data", (chunk) => {
            data.push(chunk);
          });

          response.on("end", () => {
            log(`Data for ${timestamp} has been fetched.`);
            const buffer = Buffer.concat(data);
            fs.createWriteStream(gribFilePath).write(buffer);
            resolve(gribFilePath);
          });
        } else {
          log(`Error code ${response.statusCode}`);
          reject(response.statusCode);
        }
      })
      .on("error", (error) => {
        log(`Error: ${error.message}`);
        reject(error);
      });
  });
  return promise;
}

/**
 * GFS data is published every 6 hours and and stored in 00 06 12 18 subdirectories.
 * This function returns last interval hour from any provided hour.
 * @param {String|Number} hours
 * @returns {('00'|'06'|'12'|'18')}
 */
function getClosestInterval(hours) {
  const result = Math.floor(hours / 6) * 6;
  return String(result).padStart(2, "0");
}

/**
 * Approximate GFS data publishing time according to actual publishing time on
 * https://www.nco.ncep.noaa.gov/pmb/nwprod/prodstat/index.html
 * and https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/.
 * The data is published at least 3h 40m later than cycle time.
 */
const APPROX_PUBLISH_DELAY_MS = 13320000; // 3.7 hours

/**
 * Returns nearest time relative to provided time when GFS data should be already published.
 * @param {String} isoTimestamp - ISO 8601
 * @returns {Object}
 */
function getNearestPublishedTime(isoTimestamp) {
  const time = moment(isoTimestamp).utc();
  const nearestCycle = getClosestInterval(time.hour());
  const nearestTime = moment(time).set({
    hours: parseInt(nearestCycle),
    minutes: 0,
    seconds: 0,
    milliseconds: 0,
  });
  if (moment() - nearestTime < APPROX_PUBLISH_DELAY_MS) {
    nearestTime.subtract(6, "hours");
  }
  return {
    hour: nearestTime.format("HH"),
    date: nearestTime.format("YYYYMMDD"),
    timestamp: nearestTime.format("YYYYMMDDHH"),
  };
}

/**
 * Delete older data but keep last N cycles
 */
function deleteOlderData() {
  const lastDataCyclesFiles = getIncrementalArray().map((hours) =>
    getJsonFilePath(
      getNearestPublishedTime(moment.utc().subtract(hours, "hours")).timestamp
    )
  );
  fs.readdirSync(JSON_DATA_DIR).forEach((item) => {
    const path = `${JSON_DATA_DIR}/${item}`;
    if (!lastDataCyclesFiles.includes(path)) {
      fs.unlinkSync(path);
    }
  });
}

/**
 * Checks for the latest available data relative to the provided time locally and fetches if needed.
 */
function maybeFetchGribData(time = moment.utc()) {
  const nearestPublishedTime = getNearestPublishedTime(time);
  const jsonFilePath = getJsonFilePath(nearestPublishedTime.timestamp);
  const gribFilePath = getGribFilePath(nearestPublishedTime.timestamp);
  log(
    `Target time: ${time.format()}. Nearest published time: ${
      nearestPublishedTime.timestamp
    }`
  );
  if (fs.existsSync(jsonFilePath)) {
    log(
      `Data for ${nearestPublishedTime.timestamp} exists. No need to fetch new data.`
    );
    return;
  }

  log(`New data for ${nearestPublishedTime.timestamp} due to be fetched.`);
  if (fs.existsSync(gribFilePath)) {
    convertGribToJson(gribFilePath, jsonFilePath);
  } else {
    getGribData(nearestPublishedTime.date, nearestPublishedTime.hour)
      .then(() => {
        convertGribToJson(gribFilePath, jsonFilePath);
      })
      .catch((err) => log(err));
  }
}

const app = express();

app.get("/GetWindData", (req, res) => {
  if (!moment(req.query.isoTimestamp, moment.ISO_8601).isValid()) {
    res.status(400);
    res.json({
      error:
        "Provide a valid isoTimestamp query parameter to get wind forecast data.",
    });
    return;
  }
  const nearestPublishedTime = getNearestPublishedTime(req.query.isoTimestamp);
  const jsonFilePath = getJsonFilePath(nearestPublishedTime.timestamp);

  if (!fs.existsSync(jsonFilePath)) {
    res.status(404);
    res.json({
      error: "There's no data for the specified time.",
    });
    return;
  }

  fs.readFile(jsonFilePath, (err, data) => {
    if (err) {
      res.status(400);
      res.json({
        error: "Internal error.",
      });
      return;
    }
    res.json({
      data: JSON.parse(data),
      timestamp: nearestPublishedTime.timestamp,
    });
  });
});

app.get("/GetTimestamp", (req, res) => {
  if (!moment(req.query.isoTimestamp, moment.ISO_8601).isValid()) {
    res.status(400);
    res.json({
      message:
        "Provide a valid isoTimestamp query parameter to get nearest available data timestamp.",
    });
    return;
  }

  const nearestPublishedTime = getNearestPublishedTime(req.query.isoTimestamp);
  const jsonFilePath = getJsonFilePath(nearestPublishedTime.timestamp);

  if (!fs.existsSync(jsonFilePath)) {
    res.status(404);
    res.json({ error: "There's no data for the specified time." });
    return;
  }

  res.json({ timestamp: nearestPublishedTime.timestamp });
});

function init() {
  // Create data directories and log file
  if (!fs.existsSync(GRIB_DATA_DIR)) {
    fs.mkdirSync(GRIB_DATA_DIR);
  }

  if (!fs.existsSync(JSON_DATA_DIR)) {
    fs.mkdirSync(JSON_DATA_DIR);
  }

  fs.writeFile(LOG_FILE, "", { flag: "wx" }, () => {});

  // Perform initial data fetch for last 5 data cycles
  getIncrementalArray().forEach((hours) =>
    maybeFetchGribData(moment.utc().subtract(hours, "hours"))
  );

  // Check for new data periodically
  const TEN_MINUTES = 600000;
  setInterval(() => {
    maybeFetchGribData();
    deleteOlderData();
  }, TEN_MINUTES);

  app.listen(SERVER_PORT, () => {
    log(`Server started on port ${SERVER_PORT}`);
  });
}

init();
