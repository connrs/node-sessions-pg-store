var test = require('tape');
var store = require('..');
var client = {};

test('Throw no client error', function (t) {
  t.plan(1);
  t.throws(store, /No pg client when creating session store/);
});

test('Add session', function (t) {
  var insertQuery = 'INSERT INTO session\n(uid, meta, data, last_used)\nVALUES($1, $2, $3, NOW())';
  var uid = 'AAA1';
  var meta = {};
  var data = { user_id: 1 };
  t.plan(4);
  client.query = function (query, params, done) {
    t.equal(query, insertQuery);
    t.deepEqual(uid, params[0]);
    t.deepEqual(meta, JSON.parse(params[1]));
    t.deepEqual(data, JSON.parse(params[2]));
  };
  store({client: client}).add(uid, meta, data, function () {});
});

test('Callback with error if add session error', function (t) {
  var uid = 'AAA1';
  var data = { user_id: 1 };
  t.plan(1);
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({client: client}).add(uid, null, data, function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Callback with meta and data if add session successful', function (t) {
  var uid = 'AAA1';
  var meta = {};
  var data = { user_id: 1 };
  t.plan(3);
  client.query = function (query, params, done) {
    done();
  };
  store({client: client}).add(uid, meta, data, function (err, outMeta, outData) {
    t.error(err);
    t.deepEqual(meta, outMeta);
    t.deepEqual(data, outData);
  });
});

test('Get UIDs', function (t) {
  t.plan(1);
  client.query = function (query, done) {
    t.equal(query, 'SELECT uid\nFROM session\nWHERE deleted_at IS NULL');
  };
  store({client: client}).uids(function () {});
});

test('Callback with error if get UIDs error', function (t) {
  t.plan(1);
  client.query = function (query, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({client: client}).uids(function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Callback with UIDs if successful', function (t) {
  t.plan(1);
  client.query = function (query, done) {
    done(null, {
      rows: [
        {uid: 'ABC1'},
        {uid: 'ABC2'}
      ]
    });
  };
  store({client: client}).uids(function (err, uids) {
    t.deepEqual(uids, ['ABC1', 'ABC2']);
  });
});

test('Set session data runs SELECT', function (t) {
  t.plan(2);
  client.query = function (query, params, done) {
    t.equal(query, 'SELECT meta, data\nFROM session\nWHERE deleted_at IS NULL AND uid=$1');
    t.deepEqual(params, ['ABC1']);
  };
  store({client: client}).set('ABC1', {a: 1}, {b: 2}, function () {});
});

test('Set session data error on first SELECT', function (t) {
  t.plan(1);
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({client: client}).set('ABC1', {a: 1}, {b: 2}, function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Set session data runs UPDATE', function (t) {

  client.query = function (query, params, done) {
    client.query = function (query, params, done) {
      t.equal(query, 'UPDATE session SET\ndata = $1,\nmeta = $2\nWHERE uid = $3');
      t.equal('ABC1', params[2]);
      t.deepEqual(JSON.parse(params[0]), {a: 1, z: 3});
      t.deepEqual(JSON.parse(params[1]), {b: 2, y: 4});
      done();
    };
    done(null, {
      rows: [
        {
          data: JSON.stringify({a: 1}),
          meta: JSON.stringify({b: 2})
        }
      ]
    });
  };
  store({client: client}).set('ABC1', {y: 4}, {z: 3}, function (err) {
    t.error(err);
    t.end();
  });
});

test('Set session data update returns error', function (t) {

  t.plan(1);
  client.query = function (query, params, done) {
    client.query = function (query, params, done) {
      done(new Error('PGSQL ERROR'));
    };
    done(null, {
      rows: [
        {
          data: JSON.stringify({a: 1}),
          meta: JSON.stringify({b: 2})
        }
      ]
    });
  };
  store({client: client}).set('ABC1', {y: 4}, {z: 3}, function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Get session data', function (t) {
  t.plan(2);
  client.query = function (query, params, done) {
    t.equal(query, 'SELECT meta, data\nFROM session\nWHERE uid = $1 AND deleted_at IS NULL');
    t.deepEqual(params, ['ABC1']);
  };
  store({client: client}).get('ABC1', function () {});
});

test('Get session data returns error', function (t) {
  t.plan(1);
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({client: client}).get('ABC1', function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Get session data returns meta & data', function (t) {
  t.plan(2);
  client.query = function (query, params, done) {
    done(null, {rows: [{
      meta: JSON.stringify({a: 1}),
      data: JSON.stringify({b: 2})
    }]});
  };
  store({client: client}).get('ABC1', function (err, meta, data) {
    t.deepEqual(meta, {a: 1});
    t.deepEqual(data, {b: 2});
  });
});

test('Remove session data', function (t) {
  t.plan(2);
  client.query = function (query, params, done) {
    t.equal(query, 'UPDATE session SET\nmeta = \'\',\ndata = \'\',\ndeleted_at = NOW()\nWHERE uid = $1 AND deleted_at IS NULL');
    t.deepEqual(params, ['ABC']);
  };
  store({client: client}).remove('ABC', function () {});
});

test('Remove session data return error', function (t) {
  t.plan(1);
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({client: client}).remove('ABC', function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Remove session returns no error', function (t) {
  t.plan(1);
  client.query = function (query, params, done) {
    done(null, {

    });
  };
  store({client: client}).remove('ABC', function (err) {
    t.ok(!err);
  });
});
