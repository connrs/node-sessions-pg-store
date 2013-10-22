var test = require('tape');
var store = require('..');
var pg = {
  connect: function () {}
};
var conString = 'postgres://postgres:postgres@postgres/postgres';
var client = {};

function noop() {}

test.only('Throw no client error', function (t) {
  t.plan(1);
  t.throws(store.bind(this), /No pg when creating session store/);
});

test('Throw no connection string', function (t) { t.plan(1);
  t.throws(store.bind(store, pg), /No connection string when creating session store/);
});

test('Returns error if error connecting', function (t) {
  t.plan(2);
  pg.connect = function (testConString, done) {
    t.equal(conString, testConString);
    done(new Error('PGSQL ERROR'));
  };
  store({pg: pg, conString: conString}).add('AAA1', {}, {}, function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Add session', function (t) {
  var insertQuery = 'INSERT INTO session\n(uid, meta, data, last_used)\nVALUES($1, $2, $3, NOW())';
  var uid = 'AAA1';
  var meta = {};
  var data = { user_id: 1 };
  t.plan(5);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    t.equal(query, insertQuery);
    t.deepEqual(uid, params[0]);
    t.deepEqual(meta, JSON.parse(params[1]));
    t.deepEqual(data, JSON.parse(params[2]));
    done();
  };
  store({pg: pg, conString: conString}).add(uid, meta, data, function () {});
});

test('Callback with error if add session error', function (t) {
  var uid = 'AAA1';
  var data = { user_id: 1 };
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({pg: pg, conString: conString}).add(uid, null, data, function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Callback with meta and data if add session successful', function (t) {
  var uid = 'AAA1';
  var meta = {};
  var data = { user_id: 1 };
  t.plan(4);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done();
  };
  store({pg: pg, conString: conString}).add(uid, meta, data, function (err, outMeta, outData) {
    t.error(err);
    t.deepEqual(meta, outMeta);
    t.deepEqual(data, outData);
  });
});

test('Closes client after add session', function (t) {
  var uid = 'AAA1';
  var meta = {};
  var data = { user_id: 1 };
  t.plan(1);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done();
  };
  store({pg: pg, conString: conString}).add(uid, meta, data, function (err, outMeta, outData) {
  });
});

test('Get UIDs', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    t.equal(query, 'SELECT uid\nFROM session\nWHERE deleted_at IS NULL');
    done(null, {rows: []});
  };
  store({pg: pg, conString: conString}).uids(function () {});
});

test('Callback with error if get UIDs error', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'), null, function () {});
  };
  store({pg: pg, conString: conString}).uids(function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Callback with UIDs if successful', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(null, {
      rows: [
        {uid: 'ABC1'},
        {uid: 'ABC2'}
      ]
    }, function () {});
  };
  store({pg: pg, conString: conString}).uids(function (err, uids) {
    t.deepEqual(uids, ['ABC1', 'ABC2']);
  });
});

test('Closes client after fetching UIDs', function (t) {
  t.plan(1);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(null, {
      rows: [
        {uid: 'ABC1'},
        {uid: 'ABC2'}
      ]
    });
  };
  store({pg: pg, conString: conString}).uids(function (err, uids) {
  });
});

test('Set session data runs SELECT', function (t) {
  t.plan(3);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    t.equal(query, 'SELECT meta, data\nFROM session\nWHERE deleted_at IS NULL AND uid=$1');
    t.deepEqual(params, ['ABC1']);
    done(null, {rows: []});
  };
  store({pg: pg, conString: conString}).set('ABC1', {a: 1}, {b: 2}, function () {});
});

test('Set session data error on first SELECT', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({pg: pg, conString: conString}).set('ABC1', {a: 1}, {b: 2}, function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Set session data runs UPDATE', function (t) {
  t.plan(7);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    client.query = function (query, params, done) {
      t.equal(query, 'UPDATE session SET\ndata = $1,\nmeta = $2\nWHERE uid = $3');
      t.equal('ABC1', params[2]);
      t.deepEqual(JSON.parse(params[0]), {a: 1, z: 3});
      t.deepEqual(JSON.parse(params[1]), {b: 2, y: 4});
      done(null, {rows: []});
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
  store({pg: pg, conString: conString}).set('ABC1', {y: 4}, {z: 3}, function (err) {
    t.error(err);
  });
});

test('Set session data update returns error', function (t) {
  t.plan(3);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
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
  store({pg: pg, conString: conString}).set('ABC1', {y: 4}, {z: 3}, function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Set session closes client on completion', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    client.query = function (query, params, done) {
      done(null, []);
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
  store({pg: pg, conString: conString}).set('ABC1', {y: 4}, {z: 3}, function (err) {
  });
});

test('Get session data', function (t) {
  t.plan(3);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    t.equal(query, 'SELECT meta, data\nFROM session\nWHERE uid = $1 AND deleted_at IS NULL');
    t.deepEqual(params, ['ABC1']);
    done(null, {rows: []});
  };
  store({pg: pg, conString: conString}).get('ABC1', function () {});
});

test('Get session data returns error', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({pg: pg, conString: conString}).get('ABC1', function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Get session data returns meta & data', function (t) {
  t.plan(3);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(null, {rows: [{
      meta: JSON.stringify({a: 1}),
      data: JSON.stringify({b: 2})
    }]});
  };
  store({pg: pg, conString: conString}).get('ABC1', function (err, meta, data) {
    t.deepEqual(meta, {a: 1});
    t.deepEqual(data, {b: 2});
  });
});

test('Remove session data', function (t) {
  t.plan(3);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    t.equal(query, 'UPDATE session SET\nmeta = \'\',\ndata = \'\',\ndeleted_at = NOW()\nWHERE uid = $1 AND deleted_at IS NULL');
    t.deepEqual(params, ['ABC']);
    done();
  };
  store({pg: pg, conString: conString}).remove('ABC', function () {});
});

test('Remove session data return error', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(new Error('PGSQL ERROR'));
  };
  store({pg: pg, conString: conString}).remove('ABC', function (err) {
    t.equal(err.message, 'PGSQL ERROR');
  });
});

test('Remove session returns no error', function (t) {
  t.plan(2);
  pg.connect = function (conString, done) {
    done(null, client, function () {
      t.pass();
    });
  };
  client.query = function (query, params, done) {
    done(null, {

    });
  };
  store({pg: pg, conString: conString}).remove('ABC', function (err) {
    t.ok(!err);
  });
});
