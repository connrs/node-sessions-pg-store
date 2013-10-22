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

module.exports = query;
