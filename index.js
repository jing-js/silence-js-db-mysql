'use strict';

const mysql = require('mysql2');
const path = require('path');
const util = require('silence-js-util');
const CWD = process.cwd();
const fs = require('fs');

class SqliteDatabaseStore {
  constructor(config) {
    this.logger = config.logger;
    this._db = null;
    this._host = config.host || 'localhost';
    this._user = config.user;
    this._pass = config.password;
    this._dbName = config.database;
    this._cLimit = config.connectionLimit || 10;
  }

  init() {
    return new Promise((resolve, reject) => {
      this._db = mysql.createPool({
        host: this._host,
        user: this._user,
        password: this._pass,
        database: this._dbName,
        connectionLimit: this._cLimit
      });
      // create pool will lazy connect
      resolve();
    });
  }
  close() {
    return new Promise((resolve, reject) => {
      this._db.end(err => {
        if(err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
  initField(field) {
    if (!field.rules) {
      field.rules = {};
    }

    if (!field.dbType) {
      field.dbType = field.type;
    }

    field.dbType = field.dbType.toUpperCase();

    if (field.dbType === 'STRING') {
      field.dbType === 'VARCHAR(255)';
    } else if (['DATE', 'TIME', 'DATETIME'].indexOf(field.dbType) >= 0) {
      field.dbType = 'TIMESTAMP';
    }

    if (field.dbType === 'BOOLEAN') {
      field.type = 'boolean';
    } else if (/CHAR/.test(field.dbType) || /TEXT/.test(field.dbType)) {
      let m = field.dbType.match(/^\w+\(\s*(\d+)\s*\)/);
      if (m && !field.rules.maxLength && !field.rules.rangeLength) {
        field.rules.maxLength = Number(m[1]);
      }
      field.type = 'string';
    } else if (/INT/.test(field.dbType)
      ||  /DECIMAL|NUMERIC/.test(field.dbType)
      || ['FLOAT', 'DOUBLE', 'TIMESTAMP'].indexOf(field.dbType) >= 0
    ) {
      field.type = 'number';
    } else {
      return -1;
    }

    return 0;

  }
  genCreateTableSQL(Model) {
    let segments = [];
    let pk = [];
    let indexFields = [];
    let indices = Model.indices;
    let fields = Model.fields;
    let name = Model.table;

    let _uniques = [];

    if (typeof indices === 'object' && indices !== null) {
      /*
       * {
       *    someIndex: ['someField', {anotherField: 'DESC'}],
       *    anotherIndex: [...]
       * }
       */
      for(let k in indices) {
        let f = indices[k].map(index => {
          if (typeof index !== 'object') {
            return `\`${index}\` ASC`;
          } else {
            let fn;
            let fs;
            for(fn in index) {
              fs = index[fn];
              break;
            }
            return `\`${fn}\` ${fs}`;
          }
        }).join(', ')
        indexFields.push(`INDEX \`${k}\` (${f})`);
      }
    }
    for(let i = 0; i < fields.length; i++) {

      let field = fields[i];
      let sqlSeg = `\`${field.name}\` ${field.dbType.toUpperCase()}`;

      if (field.require || field.primaryKey) {
        field.require = !field.autoIncrement;
        sqlSeg += ' NOT NULL';
      } else {
        field.require = false;
        sqlSeg += ' NULL';
      }

      if (field.hasOwnProperty('defaultValue')) {
        let dv = field.defaultValue;
        if (/CHAR/.test(field.dbType) || /TEXT/.test(field.dbType)) {
          sqlSeg += ` DEFAULT '${dv.replace(/\'/g, '\\\'')}'`;
        } else {
          sqlSeg += ` DEFAULT ${dv}`;
        }

        if (field.dbType === 'TIMESTAMP' && /CURRENT_TIMESTAMP/.test(dv)) {
          field.defaultValue = undefined;
          field.require = false;
        }
      }

      if (field.autoIncrement === true) {
        sqlSeg += ' AUTO_INCREMENT';
      }

      if (field.primaryKey) {
        pk.push(field.name);
      }

      if (field.unique) {
        let sort = field.unique === 'DESC' ? 'DESC' : 'ASC';
        _uniques.push(
          `UNIQUE INDEX \`${field.name}_UNIQUE\` (\`${field.name}\` ${sort})`
        );
      }

      if (field.index) {
        let sort = field.index === 'DESC' ? 'DESC' : 'ASC';
        indexFields.push(`INDEX \`${field.name}_INDEX\` (\`${field.name}\` ${sort})`);
      }

      segments.push(sqlSeg);
    }

    if (pk.length > 0) {
      segments.push(`PRIMARY KEY (${pk.map(p => `\`${p}\``).join(', ')})`);
    }

    if (indexFields.length > 0) {
      segments = segments.concat(indexFields);
    }

    if (_uniques.length > 0) {
      segments = segments.concat(_uniques);
    }

    //todo support foreign keys

    let sql = `CREATE TABLE \`${name}\` (\n  ${segments.join(',\n  ')});`;

    return sql;

  }
  exec(queryString, queryParams) {
    this.logger.debug(queryString);
    this.logger.debug(queryParams);
    return new Promise((resolve, reject) => {
      this._db.execute(queryString, queryParams, function(err, result) {
        if (err) {
          reject(err);
        } else {
          resolve({
            affectedRows: typeof result.changedRows === 'number' ? result.changedRows : result.affectedRows,
            insertId: result.insertId
          });
        }
      });
    });
  }
  query(queryString, queryParams) {
    this.logger.debug(queryString);
    this.logger.debug(queryParams);
    return new Promise((resolve, reject) => {
      this._db.execute(queryString, queryParams, function(err, rows) {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }
}

module.exports = SqliteDatabaseStore;
