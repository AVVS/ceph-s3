'use strict';

var _ = require('lodash');
var users = require('./users.js');
var knox = require('knox');
var async = require('neo-async');

/**
 * Create s3-compatible client with Ceph Object Storage
 * @param {Object} params:
 *   * access    {String}   `read` or `readwrite`
 *   * auth      {String}   rados gateway URL
 *   * container {String}   container to use - defaults to `arkapi`
 * @param {String} username - username for swift
 * @param {Function} done - called when S3 is ready
 */
function S3Client(params, username, done) {

    // assume its always a function
    done = done || _.noop;

    params = this.params = _.defaults(params || {}, {
        port: 6788,
        bucket: process.env.S3_BUCKET || 'arkapi',
        style: 'path',
        endpoint: process.env.S3_ENDPOINT || '10.10.100.69',
        secure: false,
        retry: [100, 1000, 2000]
    });

    var user = users.get(username);
    if (!user) {
        throw new Error('user must be specified');
    }

    params.key = user.access_key;
    params.secret = user.secret_key;

    this.s3 = knox.createClient(params);

    this.s3
        .put('/', {})
        .on('response', function s3BucketCreated(response) {
            // 200 indicates successful bucket creation
            if (response.statusCode !== 200) {
                return done(new Error('Couldn\'t create bucket'));
            }

            done();
        })
        .on('error', function s3Error(err) {
            done(err);
        })
        .end();

}


/**
 * Returns readable stream
 * @param   {String}            filename to download
 * @param   {Function}          callback <err, result[File]>
 * @return  {ReadableStream}    pipe this to where you need to, and listen to errors!
 */
S3Client.prototype.getFile = function (filename, headers, callback) {
    if (typeof headers === 'function') {
        callback = headers;
        headers = {};
    }

    headers = _.pick(headers, [
        'range',
        'if-modified-since',
        'if-unmodified-since',
        'if-match',
        'if-none-match'
    ]);

    this.s3.getFile(filename, headers, callback);
};

/**
 * Stores file in the underlaying system
 * @param {Object}   options:
 *   * filename  {String}  save with this filename
 *   * metadata  {Object}  optional kv data
 *   * headers   {Object}  optional headers to pass
 * @param {Function} callback <err, file>
 */
S3Client.prototype.storeFile = function (options, callback) {
    var fileBuffer = options.buffer,
        filename = options.filename,
        headers  = options.headers || {};

    this.s3.putBuffer(fileBuffer, filename, headers, function s3PutBufferCallback(err, res) {
        if (err) {
            return callback(err);
        }

        callback(null, res.statusCode === 200 ? filename : false);
    });

};

/**
 * Tries to convert any incoming files into objects suitable for .storeFiles
 * @param {any} files              incoming files
 * @param {object} headers         headers appended to each file
 * @param {string} prefix          prefix for saved files, ex.: user_id:121212:dump.html => prefix will be "user_id:121212"
 * @return {array}                 array of suitable objects with .filename, .headers and .buffer properties
 */
S3Client.prototype.file2obj = function (files, headers, prefix) {

    var cnt = 0, ret = [];
    headers = headers || {};
    prefix = prefix || '';

    _.each(_.isArray(files) ? files : [files], function (v) {
        if(!_.isPlainObject(v)) {
            v = { buffer: v };
        }
        if(v.buffer) {
            v.headers = _.extend({}, headers, v.headers);
            v.filename = prefix + (v.filename || ++cnt);
            if(!Buffer.isBuffer(v.buffer)) {
                v.buffer = new Buffer(v.buffer, 'utf-8');
            }
            ret.push(v);
        }
    });
    return ret;
};

/**
 * Store raw crawling result into local cerph private cloud
 *
 * @param {array|string|Buffer} files          files to store: [{ buffer, headers, filename}, {...}]
 * @param {function} callback                   <err>
 */
S3Client.prototype.storeFiles = function (files, callback) {

    var self = this, retry = this.params.retry;
    async.each(files, function (file, next) {
        var retryPntr = 0;
        async.retry(retry.length, function (done) {
            self.storeFile(file, function (err) {
                if(!err) { return done(); } // success
                setTimeout(done, retry[retryPntr++], err);
            });
        }, next);
    }, callback);

};

/**
 * Public API
 * @type {S3Client}
 */
module.exports = S3Client;
