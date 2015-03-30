'use strict';

var _ = require('lodash');

var configuration;

try {
    configuration = require('../users.json');
} catch (e) {
    configuration  = {
        test: {
            access_key: '123',
            secret_key: 'abc'
        }
    };
}

/**
 * Returns user's access_key and secret_key
 * @param  {String} username
 * @return {Object|Boolean}
 */
exports.get = function (username) {
    return configuration[username] || false;
};

/**
 * Extends user information
 * @param {Object} users
 */
exports.add = function (users) {
    _.extend(configuration, users);
};

/**
 * Removes username from configuration
 * @param  {String} username
 */
exports.remove = function (username) {
    configuration = _.without(configuration, username);
};
