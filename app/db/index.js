const {promisify} = require("es6-promisify");
const mysql = require('mysql');

class MysqlConnection {

    constructor(config) {
        this.config = config;
        this.mysqlPool = null;
    }

    async initConnection() {
        let connectionOptions = {
            host: this.config.db_host,
            user: this.config.db_user,
            password: this.config.db_password,
            database: this.config.db_name,
            charset: 'utf8mb4',
        };
        try {
            this.mysqlPool = mysql.createPool(connectionOptions);
        } catch (err) {
            throw new Error(err)
        }
    }

    async importSchools(schoolsToCreate) {
        let keysToCreate = Object.keys(schoolsToCreate[schoolsToCreate.length - 1]);
        return new Promise((resolve, reject) => {
            try {
                this.mysqlPool.getConnection((err, connection) => {
                    // Use the connection
                    let insertStatement = `INSERT INTO kindergarten (${keysToCreate.join(',')}) VALUES ?`;
                    connection.query(
                        insertStatement,
                        [schoolsToCreate.map(item => Object.values(item))],
                        (e, results) => {
                            if (e) {
                                console.log("Insert err: ", e);
                                reject(e);
                            }
                            // And done with the connection.
                            connection.release();
                            console.log("Res: ", results);
                            resolve(results);
                    });
                });
            } catch (e) {
                reject(e);
            }
        });
    }

}

module.exports = MysqlConnection;