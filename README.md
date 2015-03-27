# S3 compatible client wrapper based on knox

`npm install ceph-s3 -S`

## Usage

I'm pretty lazy and always forget configurations. So, if you add fork this project,
add a file 'users.json' to the root - then you can use these users, as they will be
loaded on startup.

Otherwise just user ./lib/users or init function to pass your user configuration

```js
var S3Client = require('ceph-s3');

// pass users that you will use with this client
s3Client.init({

    myusername: {
        access_key: '',
        secret_key: ''
    },

    wonderfuluser: {
        access_key: '',
        secret_key: ''
    }

});

// can use anything that is supported by knox - https://github.com/Automattic/knox
var usernameClient = new S3Client({ bucket: 'mrusername' }, 'myusername', function (err) {
    if (err) {
        // failed to auth / connect / etc
        throw err;
    }

    console.log('Init complete');
});

// any errors during init will be suppressed in _.noop
// // access knox directly with mrwondeful.s3 if need be
var mrwondeful = newS3Client({ bucket: 'wonderful' }, 'mrwonderful');

// the only useful abstraction besides adding simple user management
var opts = {
    buffer: new Buffer('Contents of the file', 'utf-8'),
    filename: 'useless.txt',
    headers: {} // optional
};

mrwondeful.storeFile(opts, function (err, filename) {
    if (err) {
        throw err;
    }

    console.log(filename ? 'Uploaded %s' : 'Failed', filename);
});

```
