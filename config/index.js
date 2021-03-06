require('dotenv').config()

module.exports = {
  db_host: process.env.DB_HOST,
  db_name: process.env.DB_NAME,
  db_user: process.env.DB_USER,
  db_password: process.env.DB_PASSWORD,
  GOOGLE_API_KEY: process.env.GOOGLE_API_KEY
};
