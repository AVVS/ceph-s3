'use strict';

var users = require('./lib/users.js');

/**
 * [exports description]
 * @type {RegExp}
 */
exports = module.exports = require('./lib/client.js');

/**
 * Init users configuration
 * pass object in the format:
 * {
 *    username: {
 *    		access_key: '',
 *    		secret_key: ''
 *    }
 * }
 */
exports.init = users.add;
