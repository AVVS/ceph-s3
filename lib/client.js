'use strict';

var _ = require('lodash');
var users = require('./users.js');
var knox = require('knox');
var Promise = require('bluebird');
var Backoff = require('backoff').FibonacciStrategy;
var debug = require('debug')('ceph-s3');

/**
 * Create s3-compatible client with Ceph Object Storage
 * @param {Object} params:
 *   * access    {String}   `read` or `readwrite`
 *   * auth      {String}   rados gateway URL
 *   * container {String}   container to use - defaults to `arkapi`
 * @param {String} username - username for swift
 * @param {Function} done - called when S3 is ready
 */
function S3Client(params, username) {

    _.bindAll(this);

    params = _.defaults(params || {}, {
        port: process.env.S3_PORT || 6788,
        bucket: process.env.S3_BUCKET || 'arkapi',
        style: 'path',
        endpoint: process.env.S3_ENDPOINT || '10.10.100.69',
        secure: (process.env.S3_PROTO && process.env.S3_PROTO === 'https') ? true : false
    });

    var user = users.get(username);
    if (!user) {
        throw new Error('user must be specified');
    }

    params.key = user.access_key;
    params.secret = user.secret_key;

    this.s3 = knox.createClient(params);
}

/**
 * Adds connect method, which returns promise or callback.
 * Basically it only checks whether we have access with these credentials or not
 * and creates bucket
 *
 * @param  {Function} done
 */
S3Client.prototype.connect = function (done) {
    var s3 = this.s3;

    return new Promise(function initConnectionPromise(resolve, reject) {

        s3.put('/', {})
            .on('response', function s3BucketCreated(response) {
                // 200 indicates successful bucket creation
                if (response.statusCode !== 200) {
                    return reject(new Error('Couldn\'t create bucket'));
                }

                resolve();
            })
            .on('error', reject)
            .end();

    }).nodeify(done || _.noop);
};

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

    var s3 = this.s3;

    return Promise.fromNode(function getFilePromise(done) {
        s3.getFile(filename, headers, done);
    }).nodeify(callback || _.noop);
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
        headers  = options.headers || {},
        s3 = this.s3;

    return new Promise(function storeFilePromise(resolve, reject) {
        s3.putBuffer(fileBuffer, filename, headers, function s3PutBufferCallback(err, res) {
            if (err) {
                return reject(err);
            }

            if (res.statusCode !== 200) {
                return reject(new Error('response status code is ' + res.statusCode));
            }

            resolve(filename);
        });
    }).nodeify(callback || _.noop);
};

/**
 * Tries to convert any incoming files into objects suitable for .storeFiles
 * @param {any} files              incoming files
 * @param {object} headers         headers appended to each file
 * @param {string} prefix          prefix for saved files, ex.: user_id:121212:dump.html => prefix will be "user_id:121212"
 * @return {array}                 array of suitable objects with .filename, .headers and .buffer properties
 */
S3Client.prototype.file2obj = function (files, headers, prefix) {

    // default variables
    headers = headers || {};
    prefix = prefix || '';
    files = _.isArray(files) ? files : [files];

    var iterator = 0;
    return files.map(function remapFiles(file) {

        // transform content to buffer
        if (Buffer.isBuffer(file)) {
            file = { buffer: file };
        } else if (typeof file === 'string') {
            file = { buffer: new Buffer(file, 'utf-8') };
        } else if (file && typeof file === 'object' && typeof file.buffer === 'string') {
            file.buffer = new Buffer(file.buffer, 'utf-8');
        }

        // make sure we conform to format and we have unique filenames
        if (Buffer.isBuffer(file.buffer)) {
            file.headers = _.defaults(file.headers || {}, headers);
            file.filename = prefix + (file.filename || ++iterator);
        } else {
            console.error('Input file object: ', file);
            throw new Error('you have passed malformed file object');
        }

        return file;
    });

};

/**
 * Store raw crawling result into local cerph private cloud
 *
 * @param {Array[Object]} files     - files to store: [{ buffer, headers, filename}, {...}]
 * @param {Object}        retryOpts - specify what to do on failure { initialDelay, randomisationFactor, maxDelay, retryCount }
 * @param {function}      callback  - <err>
 */
S3Client.prototype.storeFilesWithRetry = function (files, retryOpts, callback) {

    _.defaults(retryOpts, {
        initialDelay: 1000,
        randomisationFactor: 0.3,
        maxDelay: 600000
    });

    var maxRetryCount = retryOpts.retryCount || 5;
    var storeFile = this.storeFile;

    return Promise
        .resolve(files)
        .map(function uploadFile(file, index, arrLength, currentTry, backoffInstance) {

            return storeFile(file).catch(function uploadToS3Failed(err) {
                // default the current try to 0
                currentTry = currentTry || 0;

                if (currentTry >= maxRetryCount) {
                    return Promise.reject(err);
                }

                debug('error uploading to s3: ', err);

                backoffInstance = backoffInstance || new Backoff(retryOpts);
                return Promise
                    .delay(backoffInstance.next())
                    .then(function () {
                        return uploadFile(file, index, arrLength, ++currentTry, backoffInstance);
                    });
            });
        })
        .nodeify(callback || _.noop);

};

/**
 * Public API
 * @type {S3Client}
 */
module.exports = S3Client;
