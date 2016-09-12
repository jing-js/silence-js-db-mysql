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

    if (field.dbType === 'STRING') {
      field.dbType = 'VARCHAR(255)';
    }

    if (/CHAR/.test(field.dbType) || /TEXT/.test(field.dbType)) {
      let m = field.dbType.match(/^\w+\(\s*(\d+)\s*\)/);
      if (m && !field.rules) {
        field.rules = {};
      }
      if (m && !field.rules.maxLength && !field.rules.rangeLength) {
        field.rules.maxLength = Number(m[1]);
      }
      field.type = 'string';
    } else if (/INT/.test(field.dbType)
      ||  /DECIMAL|NUMERIC/.test(field.dbType)
      || ['FLOAT', 'DOUBLE', 'TIMESTAMP'].indexOf(field.dbType) >= 0
    ) {
      field.type = 'number';
    } else if (field.type === 'boolean') {
      field.dbType = 'TINYINT';
    } else {
      return -1;
    }

    if (field.autoUpdate && (field.dbType !== 'TIMESTAMP' || field._defaultValue !== 'now')) {
      return -3;
    }
    
    if (field._defaultValue !== undefined) {
      if (field.dbType === 'TIMESTAMP') {
        if (field._defaultValue !== 'now' && typeof field._defaultValue !== 'number') {
          return -2;
        }
        if (field._defaultValue === 'now') {
          field._defaultValue = Date.now;
        }
      } else if (typeof field._defaultValue !== field.type) {
        return -2;
      }
    } else if (field.isPrimaryKey && !field.autoIncrement) {
      field.require = true;
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
      let type = field.dbType.toUpperCase();
      let sqlSeg = `\`${field.name}\` ${type === 'TIMESTAMP' ? 'BIGINT UNSIGNED' : type}`;

      if (field.require || field.isPrimaryKey) {
        field.require = !field.autoIncrement;
        sqlSeg += ' NOT NULL';
      } else {
        field.require = false;
        sqlSeg += ' NULL';
      }

      if (field._defaultValue !== undefined && typeof field._defaultValue !== 'function') {
        let dv = field.defaultValue;
        if (typeof dv === 'string') {
          sqlSeg += ` DEFAULT '${dv.replace(/\'/g, '\\\'')}'`;
        } else {
          sqlSeg += ` DEFAULT ${dv}`;
        }
      }

      if (field.autoIncrement === true) {
        sqlSeg += ' AUTO_INCREMENT';
      }

      if (field.isPrimaryKey) {
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
