'use strict';

const SilenceJS = require('silence-js');
const BaseSQLDatabaseStore = SilenceJS.BaseSQLDatabaseStore;
const mysql = require('mysql');

class MysqlDatabaseStore extends BaseSQLDatabaseStore {
  constructor(config, logger) {
    super(logger);
    this.db = null;
    this.cfg = {
      connectionLimit: config.connectionLimit || 20,
      host: config.host || '127.0.0.1',
      user: config.user || 'root',
      password: config.password,
      database: config.database || config.schema
    };
  }
  init() {
    this.db = mysql.createPool(this.cfg);
    this.db.on('error', err => {
      this.logger.error('Mysql Error');
      this.logger.error(err);
    });
    return Promise.resolve();
  }
  close() {
    this.db.end();
    return Promise.resolve();
  }
  genCreateTableSQL(Model) {
    let segments = [];
    let pk = null;
    let uniqueFields = [];
    let indexFields = [];
    let indices = Model.indices;
    let fields = Model.fields;
    
    if (util.isObject(indices)) {
      for(let k in indices) {
        indexFields.push({
          name: k,
          value: Array.isArray(indices[k]) ? indices[k].join(',') : indices[k]
        });
      }
    }
    for(let i = 0; i < fields.length; i++) {

      let sqlSeg = `\`${field.name}\` ${field.type.toUpperCase()}`;

      if (field.require || field.primaryKey) {
        sqlSeg += ' NOT NULL';
      }

      if (field.hasOwnProperty('defaultValue')) {
        sqlSeg += ` DEFAULT '${field.defaultValue}'`;
      }

      if (field.primaryKey) {
        pk = field.name;
      }

      if (field.autoIncrement === true) {
        sqlSeg += ' AUTO_INCREMENT';
      }


      if (field.unique === true) {
        uniqueFields.push(field.name);
      }
      if (field.index === true) {
        indexFields.push({
          name: field.name,
          value: field.name
        });
      }

      if (field.comment) {
        sqlSeg += ` COMMENT '${field.comment || ''}'`;
      }

      segments.push(sqlSeg);
    }

    if (pk) {
      segments.push(`PRIMARY KEY (\`${pk}\`)`);
    }

    if (uniqueFields.length > 0) {
      segments.push(...uniqueFields.map(uniqueColumn => `UNIQUE INDEX \`${uniqueColumn}_UNIQUE\` (\`${uniqueColumn}\` ASC)`))
    }
    if (indexFields.length > 0) {
      segments.push(...indexFields.map(index => `INDEX \`${index.name}_INDEX\` (${index.value})`));
    }
    //todo support foreign keys

    return `CREATE TABLE \`${name}\` (\n  ${segments.join(',\n  ')});`;Type === 'sqlite' && indexFields.length > 0) {
  }
  query(queryString, queryParams) {
    this.logger.debug(queryString);
    this.logger.debug(queryParams);
    return new Promise((resolve, reject) => {
      this.db.query(queryString, queryParams, function(err, result) {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    })
  }
}

module.exports = MysqlDatabaseStore;
