const fastCsv = require('fast-csv');
const fs = require('fs');
const path = require('path');
const nodeGeocoder = require("node-geocoder");

const config = require('../config');
const MysqlConnection = require('./db');

const mysqlConnection = new MysqlConnection(config);

const geoCoderOptions = {
    provider: 'google',

    // Optional depending on the providers
    httpAdapter: 'https', // Default
    apiKey: config.GOOGLE_API_KEY,
    formatter: null         // 'gpx', 'string', ...
};

const geocoder = nodeGeocoder(geoCoderOptions);

const dataMapper = {
    nvusc: 'nvusc',
    vusc: 'vusc',
    red_izo: 'red_izo',
    zriz_kod: 'zriz_kod',
    red_pln: 'red_pln',
    red_naz: 'red_nazev',
    red_ulice: 'red_ulice',
    red_misto: 'red_misto',
    red_psc: 'red_psc',
    telefon: 'phone',
    email1: 'email',
    www: 'www',
    izo: 'izo',
    typ: 'type',
    zar_naz: 'zar_naz',
    zuj: 'zuj',
    ruian_kod: 'ruian_code',
    cilkapa: 'children_total_capacity',
    sp_skoly: 'sp_school',
    ['děti běžné třídy']: 'children_normal_class',
    ['děti spec. třídy']: 'children_special_class',
    ['děti celkem']: 'children_total_attendance',
    ['individ. Integrované']: 'children_indiv_integr',
    ['děti školy při ZZ']: 'children_zz',
    Rok: 'year',
    latitude: 'latitude',
    longitude: 'longitude',
    status: 'gps_status',
};

async function getGpsCoordinates(address) {
// Or using Promise
    return geocoder.geocode(address)
        .then(function(res) {
            return res;
        })
        .catch(function(err) {
            console.log('GPS err: ', err);
        });
}

async function importer() {
    try {
        await mysqlConnection.initConnection();
        const dataPath = path.join(__dirname, 'data/MS_gps3.csv');

        var csvStream = fs.createReadStream(dataPath);

        const schoolsToCreate = [];
        const uniqueRecordsMap = new Map();

        fastCsv
            .fromStream(csvStream, {
                headers: true,
                ignoreEmpty: true,
            })
            .on("data", async function (data) {
                let uniqueHashCalc = data.red_izo + data.izo + data.Rok;
                let existingHash = uniqueRecordsMap.get(uniqueHashCalc);
                if (!existingHash) {
                    let schoolToCreate = {
                        unique_hash: uniqueHashCalc,
                    };
                    for (let key in data) {
                        if (typeof dataMapper[key] !== 'undefined') {
                            schoolToCreate = {
                                ...schoolToCreate,
                                [dataMapper[key]]: data[key],
                            }
                        }
                    }

                    if (typeof data.latitude === 'string') {
                        if (data.latitude.indexOf(',') !== -1) {
                            data.latitude = data.latitude.replace(',', '.');
                            schoolToCreate.latitude = Number(data.latitude);
                        }
                    }
                    if (typeof data.longitude === 'string') {
                        if (data.longitude.indexOf(',') !== -1) {
                            data.longitude = data.longitude.replace(',', '.');
                            schoolToCreate.longitude = Number(data.longitude);
                        }
                    }
                    if (data.status === 'ZERO_RESULTS') {
                        const gpsCoords = await getGpsCoordinates(data.red_misto + ' ' + data.red_ulice.replace('č.p.') + ', ' + data.red_psc + ', ' + 'Czechia');
                        if (typeof gpsCoords !== 'undefined' && Array.isArray(gpsCoords) && gpsCoords.length) {
                            const coord = gpsCoords[0];
                            schoolToCreate.latitude = coord.latitude;
                            schoolToCreate.longitude = coord.longitude;
                        }
                    }
                    uniqueRecordsMap.set(schoolToCreate.unique_hash, true);
                    schoolsToCreate.push(schoolToCreate)
                } else {
                    // console.log("Nooo:", data.red_izo + ' , ' + data.izo + ' , ' + data.Rok)
                }
            })
            .on("end", async function () {
                try {
                    await mysqlConnection.importSchools(schoolsToCreate);
                } catch (err) {
                    console.log("Error csv data import.", err);
                    throw err;
                }
                process.exit()
            })
            .on("error", function (err) {
                throw err;
            });
    } catch (err) {
        console.log("Err", err);
        process.exit();
    }
}

importer()
    .then((res) => {
    })
    .catch((err) => {
        console.log("Application error: ", err)
        process.exit()
    });
