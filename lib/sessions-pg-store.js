var util = require('util');
var xtend = require('xtend');
var query = require('./queries.js');

function PgStore(options) {
  options = options || {};

  if (!(this instanceof PgStore)) {
    return new PgStore(options);
  }

  this._initPgQuery(options.pg, options.conString);
  this._initQueries(options.table || 'session');
}

PgStore.prototype.add = function (uid, meta, data, done) {
  done = done || noop;
  this._pgQuery(this._query.add, this._addParams(uid, meta, data), this._onAddQuery.bind(this, meta, data, done));
};

PgStore.prototype.uids = function (done) {
  this._pgQuery(this._query.uids, [], this._onUIDsQuery.bind(this, done));
};

PgStore.prototype.set = function (uid, meta, data, done) {
  done = done || noop;
  this._pgQuery(this._query.setSelect, [uid], this._onSetSelectQuery.bind(this, uid, meta, data, done));
};

PgStore.prototype.get = function (uid, done) {
  done = done || noop;
  this._pgQuery(this._query.getSelect, [uid], this._onGetSelect.bind(this, done));
};

PgStore.prototype.remove = function (uid, done) {
  done = done || noop;
  this._pgQuery(this._query.removeUpdate, [uid], this._onRemoveQuery.bind(this, done));
};

PgStore.prototype._initPgQuery = function (pg, conString) {
  if (!pg) {
    throw new Error('No pg when creating session store');
  }

  if (!conString) {
    throw new Error('No connection string when creating session store');
  }

  this._pg = pg.connect.bind(pg, conString);
};

PgStore.prototype._initQueries = function (table) {
  this._query = {
    add: util.format(query.add, table),
    uids: util.format(query.uids, table),
    setSelect: util.format(query.setSelect, table),
    setUpdate: util.format(query.setUpdate, table),
    getSelect: util.format(query.getSelect, table),
    removeUpdate: util.format(query.removeUpdate, table)
  };
};

PgStore.prototype._pgQuery = function (query, params, done) {
  this._pg(this._onPgConnect.bind(this, query, params, done));
};

PgStore.prototype._onPgConnect = function (query, params, done, err, client, clientDone) {
  if (err) {
    done(err, null, noop);
  }
  else {
    client.query(query, params, this._onQuery.bind(this, done, clientDone));
  }
};

PgStore.prototype._onQuery = function (done, clientDone, err, results) {
  if (err) {
    done(err, null, clientDone);
  }
  else {
    done(null, results, clientDone);
  }
};

PgStore.prototype._addParams = function (uid, meta, data) {
  return [
    uid, this._stringify(meta), this._stringify(data)
  ];
};

PgStore.prototype._stringify = function (obj) {
  return JSON.stringify(obj || {});
};

PgStore.prototype._onAddQuery = function (meta, data, done, err, results, clientDone) {
  clientDone();

  if (err) {
    done(err);
  }
  else {
    done(null, meta, data);
  }
};

PgStore.prototype._onUIDsQuery = function (done, err, results, clientDone) {
  clientDone();

  if (err) {
    done(err);
  }
  else {
    done(null, this._uidResultsArray(results));
  }
};

PgStore.prototype._uidResultsArray = function (results) {
  return results.rows.map(this._mapUIDResultRow);
};

PgStore.prototype._mapUIDResultRow = function (row) {
  return row.uid;
};

PgStore.prototype._onSetSelectQuery = function (uid, meta, data, done, err, results, clientDone) {
  clientDone();

  if (err) {
    done(err);
  }
  else if (!results.rows.length) {
    done(new Error('UID not found'));
  }
  else {
    meta = this._stringifyExtend(results.rows[0].meta, meta);
    data = this._stringifyExtend(results.rows[0].data, data);
    this._pgQuery(this._query.setUpdate, [data, meta, uid], this._onSetUpdateQuery.bind(this, done));
  }
};

PgStore.prototype._stringifyExtend = function (str, obj) {
  var data;

  try {
    data = JSON.parse(str);
  }
  catch (e) {
    data = {};
  }
  finally {
    return JSON.stringify(xtend(data, obj));
  }
};

PgStore.prototype._onSetUpdateQuery = function (done, err, results, clientDone) {
  clientDone();

  if (err) {
    done(err);
  }
  else {
    done();
  }
};

PgStore.prototype._onGetSelect = function (done, err, results, clientDone) {
  clientDone();

  if (err) {
    done(err);
  }
  else if (!results.rows.length) {
    done('UID not found');
  }
  else {
    done(null, this._parse(results.rows[0].meta), this._parse(results.rows[0].data));
  }
};

PgStore.prototype._parse = function (str) {
  var obj;

  try {
    obj = JSON.parse(str);
  }
  catch (e) {
    obj = {};
  }
  finally {
    return obj;
  }
};

PgStore.prototype._onRemoveQuery = function (done, err, results, clientDone) {
  clientDone();

  if (err) {
    done(err);
  }
  else {
    done();
  }
};

function noop() {}

module.exports = PgStore;
