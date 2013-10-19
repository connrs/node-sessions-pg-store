var util = require('util');
var xtend = require('xtend');
var query = {
  add: [
    'INSERT INTO %s',
    '(uid, meta, data, last_used)',
    'VALUES($1, $2, $3, NOW())'
  ].join('\n'),
  uids: [
    'SELECT uid',
    'FROM %s',
    'WHERE deleted_at IS NULL'
  ].join('\n'),
  setSelect: [
    'SELECT meta, data',
    'FROM %s',
    'WHERE deleted_at IS NULL AND uid=$1'
  ].join('\n'),
  setUpdate: [
    'UPDATE %s SET',
    'data = $1,',
    'meta = $2',
    'WHERE uid = $3'
  ].join('\n'),
  getSelect: [
    'SELECT meta, data',
    'FROM %s',
    'WHERE uid = $1 AND deleted_at IS NULL'
  ].join('\n'),
  removeUpdate: [
    'UPDATE %s SET',
    'meta = \'\',',
    'data = \'\',',
    'deleted_at = NOW()',
    'WHERE uid = $1 AND deleted_at IS NULL'
  ].join('\n')
};

function PgStore(options) {
  this._initClient(options.client);
  this._initQueries(options.table || 'session');
}

PgStore.prototype.add = function (uid, meta, data, done) {
  done = done || noop;
  this._client.query(this._query.add, this._addParams(uid, meta, data), this._onAddQuery.bind(this, meta, data, done));
};

PgStore.prototype.uids = function (done) {
  this._client.query(this._query.uids, this._onUIDsQuery.bind(this, done));
};

PgStore.prototype.set = function (uid, meta, data, done) {
  done = done || noop;
  this._client.query(this._query.setSelect, [uid], this._onSetSelectQuery.bind(this, uid, meta, data, done));
};

PgStore.prototype.get = function (uid, done) {
  done = done || noop;
  this._client.query(this._query.getSelect, [uid], this._onGetSelect.bind(this, done));
};

PgStore.prototype.remove = function (uid, done) {
  done = done || noop;
  this._client.query(this._query.removeUpdate, [uid], this._onRemoveQuery.bind(this, done));
};

PgStore.prototype._initClient = function (client) {
  if (!client) {
    throw new Error('No pg client when creating session store');
  }
  else {
    this._client = client;
  }
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

PgStore.prototype._addParams = function (uid, meta, data) {
  return [
    uid, this._stringify(meta), this._stringify(data)
  ];
};

PgStore.prototype._stringify = function (obj) {
  return JSON.stringify(obj || {});
};

PgStore.prototype._onAddQuery = function (meta, data, done, err) {
  if (err) {
    done(err);
  }
  else {
    done(null, meta, data);
  }
};

PgStore.prototype._onUIDsQuery = function (done, err, results) {
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

PgStore.prototype._onSetSelectQuery = function (uid, meta, data, done, err, results) {
  if (err) {
    done(err);
  }
  else if (!results.rows.length) {
    done(new Error('UID not found'));
  }
  else {
    meta = this._stringifyExtend(results.rows[0].meta, meta);
    data = this._stringifyExtend(results.rows[0].data, data);
    this._client.query(this._query.setUpdate, [data, meta, uid], this._onSetUpdateQuery.bind(this, done));
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

PgStore.prototype._onSetUpdateQuery = function (done, err) {
  if (err) {
    done(err);
  }
  else {
    done();
  }
};

PgStore.prototype._onGetSelect = function (done, err, results) {
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

PgStore.prototype._onRemoveQuery = function (done, err) {
  if (err) {
    done(err);
  }
  else {
    done();
  }
};

function noop() {}

function pgStore(options) {
  var store = new PgStore(options || {});
  return store;
}

module.exports = pgStore;
