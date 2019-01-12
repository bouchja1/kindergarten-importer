const fastCsv = require('fast-csv');
const fs = require('fs');
const path = require('path');

const config = require('../config');
const MysqlConnection = require('./db');

const mysqlConnection = new MysqlConnection(config);

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
    // TODO lat
    // TODO long
};

async function importer() {
    try {
        await mysqlConnection.initConnection();
        const dataPath = path.join(__dirname, 'data/MS.csv');

        var csvStream = fs.createReadStream(dataPath);

        const schoolsToCreate = [];

        fastCsv
            .fromStream(csvStream, {
                headers: true,
                ignoreEmpty: true,
            })
            .on("data", function (data) {
                let schoolToCreate = {
                    unique_hash: data.red_izo + data.Rok,
                };
                for (let key in data) {
                    if (typeof dataMapper[key] !== 'undefined') {
                        schoolToCreate = {
                            ...schoolToCreate,
                            [dataMapper[key]]: data[key],
                        }
                    }
                }
                schoolsToCreate.push(schoolToCreate)
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
