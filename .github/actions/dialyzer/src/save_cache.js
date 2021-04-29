const cache = require('@actions/cache');
const paths = ['_build/*/dialyxir*.plt*'];
const key = process.env.CACHE_KEY;
(async () => await cache.saveCache(paths, key))();;
