/**
 * Module dependencies.
 */
var crypto = require('crypto');

/**
 * Calculates the MD5 hash of a string.
 *
 * @param  {String} string - The string (or buffer).
 * @return {String}        - The MD5 hash.
 */
module.exports = {
    md5: function(string) {
        return crypto.createHash('md5').update(string).digest('hex');
    }
};
