## Smalljoin

A small tool, similar to the existing unix `join` tool, but with a focus on:

1. CSV and JSON handling, including splitting CSVS up by column and joining on JSON subfields (ie, for database dumps)
1. Handling an incoming stream of left data as an input, rather than loading it entirely into memory

### Example 

Given a big file such as this:
```
$ cat some-big-file
1,col1,col2,"test","{\"data\": {\"index\":\"a"}}"
2,col1,col2,"test","{\"data\": {\"index\":\"b"}}"
3,col1,col2,"test","{\"data\": {\"index\":\"c"}}"
4,col1,col2,"test","{\"data\": {\"index\":\"d"}}"
...
```

and a small file of the indexes you want to join on
```
$ cat index.csv 
a
b
```

```sh
# stream in the big file,
cat some-big-file | small-join --right index.csv \
    -left-join-column 4 \
    -left-json-subquery  'data.index'
```

gives the result:
```json
{"Left":{"Index":"a","Row":"1,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"a\"}}\""},"Right":{"IndexFileResult":{"Index":"a","Row":"a"}}}
{"Left":{"Index":"b","Row":"2,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"b\"}}\""},"Right":{"IndexFileResult":{"Index":"b","Row":"b"}}}
```

### Building

run `make`