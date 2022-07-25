import datetime
import typing
from typing import Any, List, Tuple, Union, Dict, Type, Optional, Iterable

import redis
from core.django.redis.cache import CacheKey, Timestamp
from django_redis.client import DefaultClient


# pylint: disable=too-many-public-methods
class DjangoRedisExtended(DefaultClient):
    """
    Extended version of django_redis wrapper. Implements methods for ZSET and LIST data types and some stuff.
    """

    def execute_command(self, command: str, *args, **kwargs) -> Any:
        """
        Executing some command of redis.

        :param command: Redis builtin command name
        :param args: Raw args
        :param kwargs: Raw kwargs, must be contains client (or None) key
        :return: result
        """
        client: Optional[redis.Redis] = kwargs.pop('client', None)
        if client is None:
            client = self.get_client(write=True)
        return client.execute_command(command, *args, **kwargs)

    def pipeline(self, transaction: bool = True) -> redis.client.Pipeline:
        client: Optional[redis.Redis] = self.get_client(write=True)
        pipeline = client.pipeline(transaction=transaction)
        return pipeline

    def _do_pattern(self, command: str, pattern: str, version: str = None, prefix: str = None,
                    client: Optional[redis.Redis] = None, itersize: int = 50_000) -> Optional[int]:
        pattern = self.make_pattern(pattern, version=version, prefix=prefix)
        if client is None:
            client = self.get_client(write=True)
        procedure = f'''
        local cursor="0";
        local count = 0;
        repeat
         local scanResult = redis.call("SCAN", cursor, "MATCH", KEYS[1], "COUNT", KEYS[2]);
            local keys = scanResult[2];
            for i = 1, #keys do
                local key = keys[i];
                redis.replicate_commands()
                redis.call("{command.upper()}", key);
                count = count +1;
            end;
            cursor = scanResult[1];
        until cursor == "0";
        return count
        '''

        storage_key = f'proc__{command.lower()}__pattern'
        func_sha = self.get(storage_key, version=version)
        if func_sha is None:
            func_sha = self.script(procedure, client=client)
            self.set(key=storage_key, value=func_sha, client=client)

        return self.eval(sha=func_sha, keys_and_args=(pattern, itersize), client=client)

    def delete_pattern(self, pattern: str, version: str = None, prefix: str = None,
                       client: Optional[redis.Redis] = None, itersize: int = 50000) -> Optional[int]:
        """
        Delete all keys by pattern, synchronously.

        :param pattern: Search pattern
        :param version: Key version
        :param prefix: Key prefix
        :param client: Redis-client objects (or None)
        :param itersize: Batch size for redis scan
        """
        if client is None:
            client = self.get_client(write=True)
        return self._do_pattern(command='DEL', pattern=pattern, version=version, prefix=prefix,
                                client=client, itersize=itersize)

    def unlink_pattern(self, pattern: str, version: str = None, prefix: str = None, client: redis.Redis = None,
                       itersize: int = 50_000) -> Optional[int]:
        """
        Unlink all keys by pattern, asynchronously.

        :param pattern: Search pattern
        :param version: Key version
        :param prefix: Key prefix
        :param client: Redis-client objects (or None)
        :param itersize: Batch size for redis scan
        """
        if client is None:
            client = self.get_client(write=True)
        return self._do_pattern(command='UNLINK', pattern=pattern, version=version, prefix=prefix,
                                client=client, itersize=itersize)

    def _decode_array(self, array: List[bytes]) -> List[Any]:
        return list(map(self.decode, array))

    def expire_at(self, key: CacheKey, ex: Timestamp, ms: bool = False, version: str = None,
                  client: Optional[redis.Redis] = None) -> bool:
        """
        Change ttl of key, expires at concrete time. Wrapper of EXPIREAT and PEXPIREAT funcs.

        :param key: Item-key
        :param ex: Timestamp of expiration, if ms is true, needs milliseconds timestamp
        :param ms: Enable milliseconds timestamp expiration
        :param version: Key version
        :param client: Redis-client objects (or None)
        :return: Success?
        """
        return self.execute_command('PEXPIREAT' if ms else 'EXPIREAT', self.make_key(key, version=version), ex,
                                    client=client) == 1

    def setex(self, key: CacheKey, item: typing.Any, exat: Timestamp = None, ex: datetime.timedelta = None,
              ms: bool = False, version: str = None,
              client: Optional[redis.Redis] = None) -> bool:
        """
        Extends of set() method, with support expire with seconds and by timestamp.

        :param key: Item key
        :param item: Object for caching
        :param exat: Timestamp of expiration, if ms is true, needs milliseconds timestamp
        :param ex: Expiration timedelta, if ms is true, needs milliseconds timestamp
        :param ms: Enable milliseconds timestamp expiration
        :param version: Key version
        :param client: Redis-client objects (or None)
        :return: Success?
        """
        args = [self.make_key(key, version=version), self.encode(item)]
        if exat:
            args.extend(['PXAT' if ms else 'EXAT', exat])
        elif ex:
            args.extend(['PX' if ms else 'EX', ex.seconds * 1000 if ms else ex.seconds])
        return self.execute_command('SET', *args, client=client) == 1

    # <lists>
    def linsert(self,
                key: CacheKey,
                where: str = 'BEFORE',
                ref_value: str = '1',
                value: str = '2',
                version: str = None,
                client: Optional[redis.Redis] = None):
        if client is None:
            client = self.get_client(write=True)

        return client.linsert(self.make_key(key=key, version=version),
                              where=where,
                              refvalue=self.encode(ref_value),
                              value=self.encode(value))

    def lpop(self, key: CacheKey,
             count: int = 1,
             version: str = None,
             client: Optional[redis.Redis] = None) -> List[Any]:
        """
        Removes and returns the first elements of the list stored at key.

        :param key: List key
        :param count: Count of extract items
        :param version: Key version
        :param client: Redis-client object (or None)
        :return: Array of items
        """
        cached_list = self.execute_command('LPOP', self.make_key(key, version=version), count, client=client)
        return self._decode_array(cached_list)

    def rpop(self, key: CacheKey, count: int = 1, version: str = None, client: Optional[redis.Redis] = None) \
            -> List[Any]:
        """
        Removes and returns the last elements of the list stored at key.

        :param key: List key
        :param count: Count of extract items
        :param version: Key version
        :param client: Redis-client objects (or None)
        :return: Array of items
        """
        cached_list = self.execute_command('RPOP', self.make_key(key, version=version), count, client=client)
        return self._decode_array(cached_list)

    def lpush(self, key: CacheKey, item: Any, version: str = None, client: Optional[redis.Redis] = None) -> None:
        """
        Push item to the beginning of the list.

        :param key: List key
        :param item: Object for caching
        :param version: Key version
        :param client: Redis-client object (or None)
        """
        self.execute_command('LPUSH', self.make_key(key, version=version), self.encode(item), client=client)

    def rpush(self, key: CacheKey, item: Any, version: str = None, client: Optional[redis.Redis] = None) -> None:
        """
        Push item to the end of the list.

        :param key: List key
        :param item: Object for caching
        :param version: Key version
        :param client: Redis-client object (or None)
        """
        self.execute_command('RPUSH', self.make_key(key, version=version), self.encode(item), client=client)

    def lrange(self, key: CacheKey,
               head: int,
               tail: int,
               version: str = None,
               client: Optional[redis.Redis] = None) -> List[Any]:
        """
        Get items range of list.

        :param key: List key
        :param head: Index of range head
        :param tail: Index of range tail
        :param version: Key version
        :param client: Redis-client object (or None)
        :return: array
        """
        cached_list = self.execute_command('LRANGE', self.make_key(key, version=version), head, tail, client=client)
        return self._decode_array(cached_list)

    def lindex(self, key: CacheKey, idx: int, version: str = None, client: Optional[redis.Redis] = None) -> Any:
        """
        Get item of list by index.

        :param key: List key
        :param idx: Index of item
        :param version: Key version
        :param client: Redis-client object (or None)
        :return: Decoded cached object
        """
        if client is None:
            client = self.get_client()
        return self.decode(client.lindex(self.make_key(key, version=version), idx))

    # </lists>

    # <sorted-sets>

    def zadd(self, key: CacheKey, mapping: Dict[Any, int],
             nx=False, xx=False, version: str = None, client: Optional[redis.Redis] = None) -> int:
        """
        Adds all the specified members with the specified scores to the sorted set stored at key.
        It is possible to specify multiple score / member pairs.
        If a specified member is already a member of the sorted set,
        the score is updated and the element reinserted at the right position to ensure the correct ordering.

        :param key: ZSET key
        :param mapping: Hashmap key::score
        :param nx: Only add new elements, don't update already existing items
        :param xx: Only update elements that already exist. Don't add new elements.
        :param version: Key version
        :param client: Redis-client object (or None)
        :return: Count of added items
        """
        if not mapping:
            raise ValueError
        if nx and xx:
            raise ValueError
        args = []

        if nx:
            args.append('NX')
        if xx:
            args.append('XX')

        for data, score in mapping.items():
            args.extend([score, self.encode(data)])
        return self.execute_command('ZADD', self.make_key(key, version=version), *args, client=client)

    def zcount(self, key: CacheKey, min_s: int = None, max_s: int = None, version: str = None,
               client: Optional[redis.Redis] = None) -> int:
        """
        Returns the number of elements in the sorted set at key with a score between min and max.

        :param key: ZSET key
        :param min_s: Min score
        :param max_s: Max score
        :param version: Key version
        :param client: Redis client object (or None)
        :return: Count of items
        """
        if client is None:
            client = self.get_client()
        return client.zcount(name=self.make_key(key, version=version), min=min_s or '-inf', max=max_s or '+inf')

    def zrange_by_score(self, key: CacheKey, min_s: int = None, max_s: int = None,
                        start: int = None, num: int = None, with_scores: bool = False,
                        score_cast: Type = float, version: str = None,
                        client: Optional[redis.Redis] = None) -> Union[List[Any], List[Tuple[Any, int]]]:
        """
        Returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
        The elements are considered to be ordered from low to high scores.

        :param key: ZSET key
        :param min_s: Min score
        :param max_s: Max score
        :param start: Offset
        :param num: Limit
        :param with_scores: Returns items with scores
        :param score_cast: Type for cast the returned scores
        :param version: Key version
        :param client: Redis client objects (or None)
        :return: Array of keys or Array of tuples(key, score)
        """
        if client is None:
            client = self.get_client()
        cached_result = client.zrangebyscore(self.make_key(key, version=version),
                                             min=min_s or '-inf', max=max_s or '+inf', start=start, num=num,
                                             withscores=with_scores, score_cast_func=score_cast)
        return [(self.decode(item[0]), item[1]) for item in cached_result] \
            if with_scores \
            else [self.decode(item) for item in cached_result]

    def zrange(self, key: CacheKey, start: int = 0, end: int = -1, desc: bool = False, with_scores: bool = False,
               score_cast: typing.Type = float, version: str = None,
               client: Optional[redis.Redis] = None) -> typing.Union[
        typing.List[typing.Any], typing.List[typing.Tuple[typing.Any, int]]]:
        """
        Returns the specified range of elements in the sorted set stored at zset.

        :param key: ZSET key
        :param start: Range start
        :param end: Range end
        :param desc: Revert sorting
        :param with_scores: Returns keys with scores
        :param score_cast: Type for cast the returned scores
        :param version: Key version
        :param client: Redis client object (or None)
        :return: Array of keys or Array of tuples(key, score)
        """
        if client is None:
            client = self.get_client()
        cached_result = client.zrange(name=self.make_key(key, version=version), start=start, end=end, desc=desc,
                                      withscores=with_scores, score_cast_func=score_cast)
        return [(self.decode(item[0]), item[1]) for item in cached_result] \
            if with_scores \
            else [self.decode(item) for item in cached_result]

    def zrem_range_by_score(self, key: CacheKey, min_s: int = None, max_s: int = None, version: str = None,
                            client: Optional[redis.Redis] = None) -> int:
        """
        Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
        IF min_s AND max_s IS NONE, REMOVES ALL

        :param key: ZSET key
        :param min_s: Start of score range
        :param max_s: End of score range
        :param version: Key version
        :param client: Redis client object (or None)
        :return: Count of removed items
        """
        if client is None:
            client = self.get_client(write=True)
        return client.zremrangebyscore(self.make_key(key, version=version), min=min_s or '-inf', max=max_s or '+inf')

    def zrem(self, key: CacheKey, items: List, version: str = None, client: Optional[redis.Redis] = None) -> int:
        """
        Removes the specified members from the sorted set stored at key. Non existing members are ignored.

        :param key: ZSET key
        :param items: Items keys
        :param version: Key version
        :param client: Redis client objects (or None)
        :return: Count of removed items
        """
        return client.zrem(name=self.make_key(key, version=version), *[self.encode(item) for item in items])

    # </sorted-sets>
    def rename(self, key: CacheKey, new_key: str, version: str = None, client: Optional[redis.Redis] = None) -> bool:
        """
        Renames key to newkey. It returns false when key does not exist.

        :param key: Item key
        :param new_key: New key
        :param version: Keys version
        :param client: Redis client object (or None)
        :return: Success?
        """
        if client is None:
            client = self.get_client(write=True)
        if client.exists(self.make_key(key, version=version)):
            result = client.rename(self.make_key(key, version=version), self.make_key(new_key, version=version))
        else:
            result = False
        return result

    def _encode(self, value: Any) -> Any:
        return self.encode(value)

    def eval(self,
             script: Optional[str] = None,
             sha: Optional[str] = None,
             keys_and_args: Optional[Iterable] = None,
             client: Optional[redis.Redis] = None) -> Any:
        """
        Invoke the execution of a stored procedure
        :param script: lua script
        :param sha: hash of loaded script
        :param keys_and_args: script args (#KEY, #ARGV)
        :param client:
        :return:
        """
        if client is None:
            client = self.get_client(write=True)
        assert not all([script, sha]), 'need script or sha argument'
        num_keys = len(keys_and_args)
        if script:
            result = client.eval(script, num_keys, *keys_and_args)
        elif sha:
            result = client.evalsha(sha, num_keys, *keys_and_args)
        else:
            raise ValueError('at least one argument')
        return result

    def script(self, arg: Union[str, Iterable], client: Optional[redis.Redis] = None) -> Optional[str]:
        """
        Loading or checking scripts by SHA
        :param arg: str or iterable, choices: str - SCRIPT LOAD, Iterable - SCRIPT EXISTS
        :param client:
        :return:
        """
        if client is None:
            client = self.get_client(write=True)
        if isinstance(arg, str):
            result = client.script_load(arg)
        elif isinstance(arg, (list, tuple)):
            result = client.script_exists(*arg)
        else:
            raise TypeError('"script" arg must be str or Iterable')
        return result
    # pylint: enable=too-many-public-methods