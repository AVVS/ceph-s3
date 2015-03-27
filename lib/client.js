'use strict';

var _ = require('lodash-node');
var users = require('./users.js');
var knox = require('knox');

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

    params = _.defaults(params || {}, {
        port: 6788,
        bucket: process.env.S3_BUCKET || 'arkapi',
        style: 'path',
        endpoint: process.env.S3_ENDPOINT || '10.10.100.69',
        secure: false
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
S3Client.prototype.getFile = function (filename, callback) {
    this.s3.getFile(filename, callback);
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
 * Public API
 * @type {S3Client}
 */
module.exports = S3Client;
