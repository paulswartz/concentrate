const cache = require('@actions/cache');
const paths = ['_build/*/dialyxir*.plt*'];
const key = process.env.CACHE_KEY;
const restoreKeys = [
  process.env.OTP_PREFIX,
  process.env.SYSTEM_PREFIX
];
(async () => await cache.restoreCache(paths, key, restoreKeys))();
