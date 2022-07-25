import datetime
import typing

from django_redis import cache
import redis

Timestamp = typing.NewType('Timestamp', int)
TimestampMS = typing.NewType('TimestampMS', int)
CacheKey = typing.NewType("CacheKey", str)


# pylint: disable=arguments-differ
# pylint: disable=too-many-public-methods
class RedisCacheExtended(cache.RedisCache):
    """
    Redis wrapper proxy
    """

    @cache.omit_exception
    def pipeline(self):
        return self.client.pipeline()

    @cache.omit_exception
    def execute_command(self, *args, **kwargs) -> typing.Any:
        return self.client.execute_command(*args, **kwargs)

    @cache.omit_exception
    def delete_pattern(self, pattern: CacheKey, version: str = None, prefix: str = None, client: redis.Redis = None,
                       itersize: int = None) -> None:
        return self.client.delete_pattern(pattern=pattern, version=version, prefix=prefix, client=client,
                                          itersize=itersize)

    @cache.omit_exception
    def unlink_pattern(self, pattern: CacheKey, version: str = None, prefix: str = None, client: redis.Redis = None,
                       itersize: int = None) -> None:
        return self.client.delete_pattern(pattern=pattern, version=version, prefix=prefix, client=client,
                                          itersize=itersize)

    @cache.omit_exception
    def expire_at(self, key: CacheKey, ex: datetime.timedelta, ms: bool = False, version: str = None,
                  client: redis.Redis = None) -> bool:
        return self.client.expire_at(key=key, ex=ex, ms=ms, version=version, client=client)

    @cache.omit_exception
    def setex(self, key: CacheKey, item: typing.Any, exat: Timestamp = None, ex: datetime.timedelta = None,
              ms: bool = False, version: str = None,
              client: redis.Redis = None) -> bool:
        return self.client.setex(key=key, item=item, exat=exat, ex=ex, ms=ms, version=version, client=client)

    @cache.omit_exception
    def lpop(self, key: CacheKey, count: int = 1, version: str = None, client: redis.Redis = None) -> typing.Any:
        return self.client.lpop(key=key, count=count, version=version, client=client)

    @cache.omit_exception
    def rpop(self, key: CacheKey, count: int = 1, version: str = None, client: redis.Redis = None) -> typing.Any:
        return self.client.rpop(key=key, count=count, version=version, client=client)

    @cache.omit_exception
    def lpush(self, key: CacheKey, item: typing.Any, version: str = None, client: redis.Redis = None) -> typing.Any:
        return self.client.lpush(key=key, item=item, version=version, client=client)

    @cache.omit_exception
    def rpush(self, key: CacheKey, item: typing.Any, version: str = None, client: redis.Redis = None) -> typing.Any:
        return self.client.rpush(key=key, item=item, version=version, client=client)

    @cache.omit_exception
    def lrange(self, key: CacheKey, head: int, tail: int, version: str = None, client: redis.Redis = None) -> \
            typing.List[typing.Any]:
        return self.client.lrange(key=key, head=head, tail=tail, version=version, client=client)

    @cache.omit_exception
    def lindex(self, key: CacheKey, idx: int, version: str = None, client: redis.Redis = None) -> typing.Any:
        return self.client.lindex(key=key, idx=idx, version=version, client=client)

    @cache.omit_exception
    def zadd(self, key: CacheKey, mapping: typing.Dict[typing.Any, int], nx=False, xx=False,
             version: str = None, client: redis.Redis = None) -> int:
        return self.client.zadd(key=key, mapping=mapping, nx=nx, xx=xx, version=version, client=client)

    @cache.omit_exception
    def zcount(self, key: CacheKey, min_s: int = None, max_s: int = None, version: str = None,
               client: redis.Redis = None) -> int:
        return self.client.zcount(key=key, min_s=min_s, max_s=max_s, version=version, client=client)

    @cache.omit_exception
    def zrange_by_score(self, key: CacheKey, min_s: int = None, max_s: int = None,
                        start: int = None, num: int = None, with_scores: bool = False,
                        score_cast: typing.Type = float, version: str = None,
                        client: redis.Redis = None) -> typing.Union[
        typing.List[typing.Any], typing.List[typing.Tuple[typing.Any, int]]]:
        return self.client.zrange_by_score(key=key, min_s=min_s, max_s=max_s, start=start, num=num,
                                           with_scores=with_scores, score_cast=score_cast, version=version,
                                           client=client)

    @cache.omit_exception
    def zrange(self, key: CacheKey, start: int = 0, end: int = -1, desc: bool = False, with_scores: bool = False,
               score_cast: typing.Type = float, version: str = None,
               client: redis.Redis = None) -> typing.Union[
        typing.List[typing.Any], typing.List[typing.Tuple[typing.Any, int]]]:
        return self.client.zrange(key=key, start=start, end=end, desc=desc, with_scores=with_scores,
                                  score_cast=score_cast, version=version, client=client)

    @cache.omit_exception
    def zrem_range_by_score(self, key: CacheKey, min_s: int = None, max_s: int = None, version: str = None,
                            client: redis.Redis = None) -> int:
        return self.client.zrem_range_by_score(key=key, min_s=min_s, max_s=max_s, version=version, client=client)

    @cache.omit_exception
    def zrem(self, key: CacheKey, items: typing.List, version: str = None, client: redis.Redis = None) -> int:
        return self.client.zrem(key=key, items=items, version=version, client=client)

    @cache.omit_exception
    def rename(self, key: CacheKey, new_key: str, version: str = None, client: redis.Redis = None) -> bool:
        return self.client.rename(key=key, new_key=new_key, version=version, client=client)

    @cache.omit_exception
    def eval(self,
             script: typing.Optional[str] = None,
             sha: typing.Optional[str] = None,
             keys_and_args: typing.Optional[typing.Iterable] = None,
             client: typing.Optional[redis.Redis] = None) -> typing.Any:
        return self.client.eval(script=script, sha=sha, keys_and_args=keys_and_args, client=client)

    @cache.omit_exception
    def script(self, arg: typing.Union[str, typing.Iterable], client: typing.Optional[redis.Redis] = None) -> str:
        return self.client.script(arg, client)

    @cache.omit_exception
    def _encode(self, value: typing.Any) -> typing.Any:
        return self.client.encode(value)
# pylint: enable=arguments-differ
# pylint: enable=too-many-public-methods